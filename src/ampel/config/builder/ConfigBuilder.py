#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/ConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 15.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib, re, json
from typing import Dict, Callable, List, Union, Sequence
from ampel.common.AmpelUtils import AmpelUtils
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.config.ConfigUtils import ConfigUtils
from ampel.config.builder.BaseConfig import BaseConfig
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.T2RunConfigCollector import T2RunConfigCollector
from ampel.model.template.NestedProcsChannelTemplate import NestedProcsChannelTemplate
from ampel.config.collector.ProcessConfigCollector import ProcessConfigCollector
from ampel.config.collector.ChannelConfigCollector import ChannelConfigCollector
from ampel.config.builder.ProcessMorpher import ProcessMorpher


class ConfigBuilder:
	"""
	Builds a central configuration dict for ampel.
	Typically, the output of this class (method build_config())
	is used as input for the method set_config() of an AmpelConfig instance.
	The method add_passwords(...) allows to add a list of passwords used to later
	decrypt encrypted config entries.
	"""

	def __init__(self, verbose: bool = False, logger: AmpelLogger = None):
		""" """
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.base_config = BaseConfig(logger, verbose)
		self.templates = {}
		self.pwd = []
		self.verbose = verbose
		self.error = False


	def load_ampel_conf(self, arg: Dict, dist_name: str = None) -> None:
		""" """

		# ("channel", "db", "resource", "pwd")
		for k in self.base_config.general_keys:
			if k in arg:
				self.base_config[k].add(arg[k], dist_name)

		for k in ("t0", "t1", "t2", "t3"):
			if k in arg:
				# ("controller", "processor", "unit", "alias", "process")
				for kk in self.base_config.tier_keys:
					if kk in arg[k]:
						self.base_config[k][kk].add(
							arg[k][kk], dist_name
						)

		if "template" in arg:
			self.register_channel_templates(arg['template'], dist_name)


	def load_conf_section(self, section: str, arg: Dict, dist_name: str = None) -> None:
		""" 
		Depending on the value of parameter 'section', a structure may be expected for dict 'arg'.
		1) tier-less sections: "channel", "db", "resource"
		-> no structure imposed
		2) tier-dependent sub-sections: "controller", "processor", "unit", "alias", "process"
		-> arg must have the following JSON structure:
		{"t0": { ... }, "t1": { ... }, ...}
		whereby the t0, t1, t2 and t3 keys are optional (at least one is required though)	
		"""

		# ("channel", "db", "resource")
		if section in self.base_config.general_keys:
			self.base_config[section].add(arg, dist_name)
			return

		# ("controller", "processor", "unit", "alias", "process")
		if section in self.base_config.tier_keys:
			for k in ("t0", "t1", "t2", "t3"):
				if k in arg:
					self.base_config[k][section].add(
						arg[k], dist_name
					)

		raise ValueError("Unknown config section: " + section)


	def register_channel_templates(self, chan_templates: Dict[str, str], dist_name: str = None) -> None:
		""" """

		if not isinstance(chan_templates, dict):
			raise ValueError("Provided argument must be a dict instance")

		for k, v in chan_templates.items():

			if k in self.templates:
				raise ValueError("Duplicated channel template: " + k)

			if self.verbose:
				self.logger.verbose(
					f"-> Registering template '{k}' " +
					ConfigCollector.distrib_hint(dist_name)
				)
			
			self.templates[k] = getattr(
				importlib.import_module(v),
				v.split(".")[-1]
			)


	def add_passwords(self, arg: Union[str, List[str]]) -> None:
		""" """
		# Not using set() as is not directly json encodable
		for el in AmpelUtils.iter(arg):
			if el not in self.base_config['pwd']:
				self.base_config['pwd'].append(el)


	def build_config(self, ignore_errors: bool = False) -> Dict:
		"""
		Builds the final ampel config using previously collected config pieces (contained in self.base_config)
		This involves a multi-step process where the config is 'morphed' its final structure.
		:raises: ValueError if self.error is True - this behavior can be disabled using the parameter ignore_errors
		"""
		if self.base_config.has_nested_error():
			if not ignore_errors:
				raise ValueError(
					"Error occured while building config, you can use the option ignore_errors=True \n" +
					"to bypass this exception and get the (possibly non-working) config nonetheless"
				)

		out = BaseConfig(self.base_config.logger, self.verbose)

		# there could be a nice way to do this
		out = self.base_config.copy()

		# Add t2 run config collector (in which both hashed values of t2 run configs 
		# and t2 run config will be added)
		out['t2']['runConfig'] = T2RunConfigCollector(
			tier=2, conf_section="runConfig", logger=self.logger, verbose=self.verbose
		)

		# Add (possibly transformed) processes to output config 
		for tier in (0, 1, 2, 3):

			p_collector = ProcessConfigCollector(
				tier=tier, conf_section="process", 
				logger=self.logger, verbose=self.verbose
			)

			# We overwrite the previous collector (out = self.base_config.copy()) 
			# with a new empty one
			out[f"t{tier}"]['process'] = p_collector

			# For each process collected before, apply transformations
			# and add it to our (almost) final process collector.
			# 'almost' because a gathering of T0 processes may occure later
			for p in self.base_config[f"t{tier}"]["process"]:
				p_collector.add(
					self.morph_process(p) \
						.apply_template() \
						.scope_aliases(self.base_config) \
						.hash_t2_run_config(out) \
						.get()
				)

		# Setup empty channel collector
		out['channel'] = ChannelConfigCollector(
			conf_section="channel", logger=self.logger, verbose=self.verbose
		)

		# Fill it with (possibly transformed) channels
		for chan_name, chan_dict in self.base_config['channel'].items():

			# Get possibly required template for this particular channel
			tpl = self._get_channel_tpl(chan_dict)

			# If templating is required
			if tpl:

				# Extract channel definition from template instance
				out['channel'].add(
					tpl.get_channel(self.logger)
				)

				# Check if processes exist embedded in channel def
				for p in tpl.get_processes(self.logger):

					# Add transformed process to final process collector
					out[f"t{p['tier']}"]['process'].add(
						self.morph_process(p) \
							.apply_template() \
							.scope_aliases(self.base_config) \
							.hash_t2_run_config(out) \
							.enforce_t3_channel_selection(chan_name) \
							.get()
					)

			else:

				# Raw/Simple/Standard channel definition
				# (encouraged behavior actually)
				out['channel'].add(chan_dict)
				

		self.logger.info("Done building config")
		return out


	def morph_process(self, process: Dict) -> ProcessMorpher:
		""" 
		Returns an instance of ProcessMorpher using the provided 
		process dict and the internal logger and templates
		"""
		return ProcessMorpher(
			process, self.templates, self.logger, self.verbose
		)


	def _get_channel_tpl(self, chan_dict):
		"""
		Internal method used to check if a template (and which one) 
		shoud be applied to a given channel dict.
		"""
		if "process" in chan_dict:
			return NestedProcsChannelTemplate(**chan_dict)

		if "template" in chan_dict:
			if chan_dict['template'] not in self.templates:
				raise ValueError(f"Unknown template: {chan_dict['template']}")
			return self.templates[chan_dict['template']](**chan_dict)

		return None


	def gather_processes(
		self, config: BaseConfig, tier: int, match: str, collect: str
	) -> Dict:
		"""
		:param channel_names:
		- None: all the available channels from the ampel config will be loaded
		- String: channel with the provided id will be loaded
		- List of strings: channels with the provided ids will be loaded 
		"""

		processes = [
			el for el in config[f't{tier}']['process'].values()
			if re.match(match, el.get('processName'))
		]

		if len(processes) <= 1:
			return None

		init_configs = []
		dist_names = set()
		out_proc = None

		for p in processes:

			if not p.get('active', False):
				self.logger.info(f"Ignoring deactivated process {p['processName']}")
				continue

			# Use first active proces as template for the multi-channel process
			# to be generated by this method
			if not out_proc:
				# Faster deep copy
				out_proc = json.loads(json.dumps(p))
				
			# Inactivate this particular process as it will be run in a multi-channel process
			self.logger.info(f"Deactivating process {p['processName']}")
			p['active'] = False

			# Gather init config
			init_configs.append(
				AmpelUtils.get_by_path(p, collect)
			)

			# Collect distribution name
			if p.get("distName"):
				dist_names.add(p.get("distName"))

		# for T0 processes: collect=processor.initConfig.channel
		ConfigUtils.set_by_path(out_proc, collect, init_configs)
		out_proc['processName'] = match
		out_proc['distName'] = "/".join(dist_names)

		return out_proc


	def print(self) -> None:
		"""
		"""
		self.base_config.print()
