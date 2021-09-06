#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 06.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib, re, json, datetime, getpass
from typing import Dict, List, Any, Optional, Set, Iterable, Union

from ampel.util.mappings import get_by_path, set_by_path, dictify, walk_and_process_dict
from ampel.log.utils import log_exception
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE, DEBUG, ERROR
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.T02ConfigCollector import T02ConfigCollector
from ampel.template.ChannelWithProcsTemplate import ChannelWithProcsTemplate
from ampel.config.collector.ProcessConfigCollector import ProcessConfigCollector
from ampel.config.collector.ChannelConfigCollector import ChannelConfigCollector
from ampel.config.builder.ProcessMorpher import ProcessMorpher
from ampel.secret.AESecret import AESecret
from ampel.secret.AESecretProvider import AESecretProvider


class ConfigBuilder:
	"""
	Builds a central configuration dict for ampel. Config building is a two pass process:
	First, all available configuration files are loaded from different repositories
	and information merged together (into the instance self.first_pass_config)
	Then, the config is 'morphed' into its final structure by a multi-step process (method build_config)
	"""

	_default_processes = ["DefaultT2Process", "DefaultPurge"]

	def __init__(self, logger: AmpelLogger = None, verbose: bool = False):

		self.logger = AmpelLogger.get_logger(
			console={'level': DEBUG if verbose else ERROR}
		) if logger is None else logger
		self.first_pass_config = FirstPassConfig(self.logger, verbose)
		self.templates: Dict[str, Any] = {}
		self.verbose = verbose
		self.error = False


	def load_ampel_conf(self,
		d: Dict,
		dist_name: str,
		version: Union[str, float, int],
		register_file: str
	) -> None:

		if self.verbose:
			self.logger.log(VERBOSE, f"Loading global ampel conf ({register_file}) from repo {dist_name}")

		# "mongo" "logging" "channel" "unit" "process" "alias" "resource"
		for k in self.first_pass_config.conf_keys:

			if k not in d:
				continue

			if k in ('unit', 'process', 'alias'):
				if isinstance(d[k], list):
					self.first_pass_config[k].add(d[k], dist_name, version, register_file)
				elif isinstance(d[k], dict):
					# kk = 't0', 't1', 't2', 't3', ... for root key "process" or "alias"
					for kk, v in d[k].items():
						if kk in self.first_pass_config[k]:
							if self.verbose:
								self.logger.log(VERBOSE, f"Parsing {k}.{kk}")
							self.first_pass_config[k][kk].add(v, dist_name, version, register_file)
						else:
							self.logger.error(f"Unknown config element: {k}.{kk}")

			else:
				self.first_pass_config[k].add(d[k], dist_name, version, register_file)

		if 'template' in d:
			self.register_channel_templates(d['template'], dist_name, version, register_file)


	'''
	def load_conf_section(self,
		section: str, arg: Dict, register_file: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:
		"""
		Depending on the value of parameter 'section', a structure may be expected for dict 'arg'.
		1) tier-less sections: 'channel', 'mongo', 'resource'
		-> no structure imposed
		2) tier-dependent sub-sections: 'controller', 'processor', 'unit', 'alias', 'process'
		-> arg must have the following JSON structure:
		{'t0': { ... }, 't1': { ... }, ...}
		whereby the t0, t1, t2 and t3 keys are optional (at least one is required though)
		"""

		# ('channel', 'mongo', 'resource')
		if section in self.first_pass_config.general_keys:
			self.first_pass_config[section].add(
				arg, register_file=register_file, dist_name=dist_name
			)
			return

		# ('controller', 'processor', 'unit', 'alias', 'process')
		if section in self.first_pass_config.tier_keys:
			for k in ('t0', 't1', 't2', 't3'):
				if k in arg:
					self.first_pass_config[k][section].add(
						arg[k], register_file=register_file, dist_name=dist_name
					)

		raise ValueError(f'Unknown config section: {section}')
	'''


	def register_channel_templates(self,
		chan_templates: Dict[str, str],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str
	) -> None:

		if not isinstance(chan_templates, dict):
			raise ValueError('Provided argument must be a dict instance')

		for k, v in chan_templates.items():

			if k in self.templates:
				raise ValueError('Duplicated channel template: ' + k)

			if self.verbose:
				self.logger.log(VERBOSE,
					f'Registering template "{k}" ' +
					register_file if register_file else '' +
					ConfigCollector.distrib_hint(distrib=dist_name)
				)

			self.templates[k] = getattr(
				importlib.import_module(v),
				v.split('.')[-1]
			)


	def build_config(self,
		stop_on_errors: int = 2,
		config_validator: Optional[str] = "ConfigChecker",
		skip_default_processes: bool = False,
		json_serializable: bool = True,
		pwds: Optional[Iterable[str]] = None,
		save: Union[bool, str, None] = None,
		sign: int = 6,
	) -> Dict[str, Any]:
		"""
		Pass 2.
		Builds the final ampel config using previously collected config pieces (contained in self.first_pass_config)
		This involves a multi-step process where the config is 'morphed' into its final structure.

		:param stop_on_errors: by default, config building stops and raises an exception if an error occured.
			- 2: stop on errors
			- 1: ignore errors in first_pass_config only (will stop on morphing/scoping/template errors)
			- 0: ignore all errors
		Note that issues might/will later arise with your ampel system.

		:param pwds: config section 'resource' might contain AES encrypted entries.
		If passwords are provided to this method, thoses entries will be decrypted.

		:param json_serializable: if True, stringify int keys in section 'confid' and potential int channel names.

		:param skip_default_processes: set to True to discard default processes defined by ampel-core.
		The static variable ConfigBuilder._default_processes references those processes by name.
		Set skip_default_processes=True if your repositories define their own default T2/T3 processes.

		:param sign: append truncated file signature (last n digits) to filename. Ex: ampel_conf_4a72fd.yaml

		:raises: ValueError if self.error is True - this behavior can be disabled using the parameter stop_on_errors
		"""

		if self.first_pass_config.has_nested_error():
			if stop_on_errors > 1:
				raise ValueError(
					'Error were reported in first pass config, you can use the option stop_on_errors = 1 (or 0)\n' +
					'to bypass this exception and get the (possibly non-working) config nonetheless'
				)

		out = {
			k: self.first_pass_config[k]
			for k in FirstPassConfig.conf_keys.keys()
		}

		out['process'] = {}

		# Add t2 init config collector (in which both hashed values of t2 run configs
		# and t2 init config will be added)
		out['confid'] = T02ConfigCollector(
			conf_section='confid', logger=self.logger, verbose=self.verbose
		)

		# Add (possibly transformed) processes to output config
		for tier in (0, 1, 2, 3, "ops"):

			tier_name = tier if tier == "ops" else f"t{tier}"

			if self.verbose:
				self.logger.log(VERBOSE, f'Checking standalone {tier_name} processes')

			p_collector = ProcessConfigCollector(
				tier=tier, conf_section='process', # type: ignore[arg-type]
				logger=self.logger, verbose=self.verbose
			)

			#out['process'][f't{tier}'] = {
			#	k: self.first_pass_config[f't{tier}'][k]
			#	for k in FirstPassConfig.tier_keys if k != 'process'
			#}

			# New empty process collector to gather morphed processes
			out['process'][tier_name] = p_collector

			# For each process collected before, apply transformations
			# and add it to our (almost) final process collector.
			# 'almost' because a gathering of T0 processes may occure later
			for p in self.first_pass_config['process'][tier_name].values():

				if skip_default_processes and p["name"] in self._default_processes:
					self.logger.info(f'Skipping {tier_name} default processes: {p["name"]}')
					continue

				if self.verbose:
					self.logger.log(VERBOSE, f'Morphing standalone {tier_name} processes: {p["name"]}')
					pass

				try:
					p_collector.add(
						self.new_morpher(p) \
							.scope_aliases(self.first_pass_config) \
							.apply_template(self.first_pass_config) \
							.hash_t2_config(out) \
							.generate_version(self.first_pass_config) \
							.get(),
						p.get('distrib'),
						p.get('version'),
						p.get('source')
					)
				except Exception as e:
					self.logger.error(f'Unable to morph process {p["name"]}', exc_info=e)
					if stop_on_errors > 0:
						raise e

		# Setup empty channel collector
		out['channel'] = ChannelConfigCollector(
			conf_section='channel', logger=self.logger, verbose=self.verbose
		)

		morph_errors = []

		# Fill it with (possibly transformed) channels
		for chan_name, chan_dict in self.first_pass_config['channel'].items():

			if not chan_dict.get('active', False):
				self.logger.info(f'Ignoring deactivated channel: {chan_name}')
				continue

			tpl = None

			# Template processing is required for this particular channel
			try:
				tpl = self._get_channel_tpl(chan_dict)
			except Exception as ee:
				log_exception(self.logger, msg=f'Unable to load template ({chan_name})', exc=ee)
				if stop_on_errors > 0:
					raise ee
				continue

			# Template processing is required for this particular channel
			if tpl:

				# Extract channel definition from template instance
				try:
					out['channel'].add(
						(c := tpl.get_channel(self.logger)),
						c.get('distrib'), c.get('version'), c.get('source')
					)
				except Exception as ee:
					log_exception(self.logger, msg=f'Unable to get channel from template ({chan_name})', exc=ee)
					if stop_on_errors > 0:
						raise ee
					continue

				# Extract process definition from template instance
				ps = []
				try:
					ps = tpl.get_processes(self.logger, self.first_pass_config)
				except Exception as ee:
					log_exception(self.logger, msg=f'Unable to get processes from template ({chan_name})', exc=ee)
					if stop_on_errors > 0:
						raise ee
					continue

				# Retrieve processes possibly embedded in channel def
				for p in ps:

					if self.verbose:
						self.logger.log(VERBOSE,
							f'Morphing channel embedded t{p["tier"]} process: {p["name"]}'
						)

					try:
						# Add transformed process to final process collector
						out['process'][f't{p["tier"]}'].add(
							self.new_morpher(p) \
								.scope_aliases(self.first_pass_config) \
								.apply_template(self.first_pass_config) \
								.hash_t2_config(out) \
								.enforce_t3_channel_selection(chan_name) \
								.generate_version(self.first_pass_config) \
								.get(),
							p.get('distrib'),
							p.get('version'),
							p.get('source')
						)
					except Exception as ee:
						morph_errors.append(p["name"])
						log_exception(
							self.logger, exc=ee,
							msg=f'Unable to morph embedded process {p["name"]} (from {p["source"]})'
						)
						if stop_on_errors > 0:
							raise ee

			else:

				# Raw/Simple/Standard channel definition
				# (encouraged behavior actually)
				out['channel'].add(
					chan_dict, chan_dict.get('distrib'),
					chan_dict.get('version'), chan_dict.get('source')
				)

		# Optionaly decrypt aes encrypted config entries
		if pwds:

			self.logger.info('Resolving AES secrets')
			sp = AESecretProvider(pwds)
			enc_confs: list[tuple[dict, str, AESecret, str]] = []
			walk_and_process_dict(
				arg = out,
				callback = self._gather_aes_config_callback,
				enc_confs = enc_confs
			)

			for tup in enc_confs:
				self.logger.info(f"Resolving {tup[3]}")
				d = tup[0]
				k = tup[1]
				secret = tup[2]
				if not sp.tell(secret, str):
					self.logger.info(" -> Secret not resolvable with specified password(s)")
				else:
					d[k] = secret.get()

	
		# Register templates in config (might be used by the 'ampel job' CLI)
		out['template'] = {k: v.__module__ for k, v in self.templates.items()}
		self.logger.info('Done building config')

		# Error Summary
		if out['unit'].err_fqns:
			self.logger.info('Erroneous units (import failed):')
			for el in out['unit'].err_fqns:
				self.logger.info(el) # type: ignore[arg-type]

		if morph_errors:
			self.logger.info('Erroneous process definitions (morphing failed):')
			for el in morph_errors:
				self.logger.info(el) # type: ignore[arg-type]
		
		# Cast into plain old dicts
		d = {
			'build': {
				'date': (now := datetime.datetime.now()).strftime("%d/%m/%Y"),
				'time': now.strftime("%H:%M:%S"),
				'by': getpass.getuser()
			}
		} | dictify(out)

		# Cosmetic: sort units first by category, then alphabetically
		u = d['unit']
		d['unit'] = {}
		for el in ("AbsProcessController", "AbsEventUnit", "ContextUnit", "LogicalUnit"):
			dd = {k: u[k] for k in sorted(u.keys()) if el in u[k]['base']}
			d['unit'] |= dd
			for k in dd:
				del u[k]
		d['unit'] |= u # Aux units

		# Convert int keys to str (ensures JSON compatibility)
		if json_serializable:

			for k in [el for el in out['channel'].keys() if isinstance(el, str) and el.isdigit()]:
				out['channel'][str(k)] = out['channel'].pop(k)
			for k in list(out['confid'].keys()):
				out['confid'][str(k)] = out['confid'].pop(k)

		if config_validator:

			from importlib import import_module
			validator = getattr(
				import_module("ampel.config.builder." + config_validator),
				config_validator
			)(d, self.logger, self.verbose)
			return validator.validate()

		if save:

			import pathlib, yaml # type: ignore
			path = pathlib.Path(save if isinstance(save, str) else 'ampel_conf.yaml')
			with open(path, 'w') as file:
				yaml.dump(d, file, sort_keys=False)

			if sign:
				import hashlib, pathlib
				h = hashlib.blake2b(path.read_bytes()).hexdigest()[:sign]
				path = path.rename(path.with_stem(f"{path.stem}_{h}"))

			self.logger.log(VERBOSE, f'Config file saved as {path}')


		return d


	def new_morpher(self, process: Dict[str, Any]) -> ProcessMorpher:
		"""
		Returns an instance of ProcessMorpher using the provided
		process dict and the internal logger and templates
		"""
		return ProcessMorpher(
			process, self.templates, self.logger, self.verbose
		)


	def _get_channel_tpl(self, chan_dict: Dict[str, Any]) -> Optional[AbsChannelTemplate]:
		"""
		Internal method used to check if a template (and which one)
		should be applied to a given channel dict.
		"""

		if 'template' in chan_dict:

			if self.verbose:
				self.logger.log(VERBOSE,
					f'Channel {chan_dict["channel"]} ({chan_dict["source"]}) '
					f'declares template: {chan_dict["template"]}'
				)

			if chan_dict['template'] not in self.templates:
				raise ValueError(f'Unknown template: {chan_dict["template"]}')

			return self.templates[chan_dict['template']](**chan_dict)


		if 'process' in chan_dict:
			return ChannelWithProcsTemplate(**chan_dict)

		return None


	def gather_processes(self,
		config: FirstPassConfig, tier: int, match: str, collect: str
	) -> Optional[Dict]:
		"""
		:param channel_names:
		- None: all the available channels from the ampel config will be loaded
		- String: channel with the provided id will be loaded
		- List of strings: channels with the provided ids will be loaded
		"""

		processes: List[Dict] = [
			el for el in config[f't{tier}']['process'].values()
			if re.match(match, el.get('name'))
		]

		if len(processes) <= 1:
			return None

		init_configs = []
		dist_names: Set[str] = set()
		out_proc: Optional[Dict] = None

		for p in processes:

			if not p.get('active', False):
				self.logger.info(f'Ignoring deactivated process: {p["name"]}')
				continue

			# Use first active proces as template for the multi-channel process
			# to be generated by this method
			if not out_proc:
				# Faster deep copy
				out_proc = json.loads(json.dumps(p))

			# Inactivate this particular process as it will be run in a multi-channel process
			self.logger.info(f'Deactivating process: {p["name"]}')
			p['active'] = False

			# Gather init config
			init_configs.append(
				get_by_path(p, collect)
			)

			# Collect distribution name
			if p.get('distrib'):
				dist_names.add(p.get('distrib')) # type: ignore

		if out_proc is None:
			return None

		# for T0 processes: collect=processor.config.directives
		set_by_path(out_proc, collect, init_configs)
		out_proc['name'] = match
		out_proc['distrib'] = '/'.join(dist_names)

		return out_proc


	def print(self) -> None:
		self.first_pass_config.print()


	def _gather_aes_config_callback(self, path, k, d, **kwargs) -> None:

		if d[k] and 'iv' in d[k]:
			try:
				secret = AESecret(**k[d])
			except Exception:
				return
			# dict, key, secret, string path (debug)
			kwargs['enc_confs'].append((d, k, secret, f"{path}.{k}"))
