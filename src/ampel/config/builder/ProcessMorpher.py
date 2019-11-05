#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/ProcessMorpher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 30.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Dict, List
from ampel.common.AmpelUtils import AmpelUtils
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.config.utils.ConfigUtils import ConfigUtils
from ampel.config.builder.BaseConfig import BaseConfig


class ProcessMorpher:
	"""
	Applies various transformations to process dicts
	"""

	def __init__(
		self, process: Dict, templates: List, logger: AmpelLogger, 
		verbose: bool = False, deep_copy: bool = False
	):
		""" """
		self.process = json.loads(json.dumps(process)) if deep_copy else process 
		self.templates = templates
		self.logger = logger
		self.verbose = verbose


	def get(self):
		""" """
		return self.process


	def enforce_t3_channel_selection(self, chan_name: str) -> 'ProcessMorpher':
		""" 
		"""
		if self.process['tier'] == 3:
			# processes embedded in channel must feature a transient selection.
			# An exception will be raised if someone embeds an admin t3 proc (without selection) 
			# within a channel. This feature is not / should not be supported
			self.process['processor']['initConfig']['select']['initConfig']['channels'] = chan_name

			if self.verbose:
				self.logger.verbose(
					f" -> Applying channel selection criteria ({chan_name}) "
					f"for process {self.process['processName']}"
				)

		return self


	def apply_template(self) -> 'ProcessMorpher':
		"""
		Applies defined template if process defines it
		"""
		# The process embedded in channel def requires templating itself
		if "template" in self.process:

			if self.verbose:
				self.logger.info(
					f" -> Applying template {self.process['template']} "
					f"to process {self.process['processName']}"
				)
			
			self.process = self.templates[
				self.process['template']
			](**self.process).get_process(self.logger)

		return self


	def _scope_alias_callback(self, path, k, d, **kwargs):
		""" """

		if 'base_config' not in kwargs:
			raise ValueError("Parameter 'base_config' missing in kwargs")

		v = d[k]

		if v and isinstance(v, str) and v[0] != "%":

			scoped_alias = f"{self.process['distName']}/{v}"

			if not any([
				scoped_alias in kwargs['base_config'][f"t{tier}"]['alias']
				for tier in (0, 1, 2, 3)
			]):
				raise ValueError(
					f"Alias {scoped_alias} not found"
				)

			# Overwrite
			d[k] = scoped_alias

			if self.verbose:
				self.logger.info(f"Alias {v} renamed into {scoped_alias}")


	def scope_aliases(self, base_config: Dict) -> 'ProcessMorpher':
		""" 
		"""
		if self.process.get('distName'):

			ConfigUtils.walk_and_process_dict(
				arg=self.process, 
				callback=self._scope_alias_callback,
				match=("initConfig", "runConfig", "init_config", "run_config"),
				base_config=base_config
			)

		else:

			self.logger.info(
				f"Cannot scope aliases for process {self.process['processName']}"
				f" as it is missing a distribution name"
			)

		return self


	def _hash_t2_run_config_callback(self, path, k, d, **kwargs) -> None:
		""" 
		"""

		out_config = kwargs.get('out_config')

		if not out_config:
			raise ValueError("Parameter 'out_config' missing in kwargs")

		for t2 in AmpelUtils.iter(d[k]):

			rc = t2.get('runConfig', None)

			if not rc:
				continue

			# alias
			if isinstance(rc, str):
				if (rc not in out_config["t2"]['alias']):
					raise ValueError(
						f"Unknown T2 run config alias ({rc}) defined in process {self.process['processName']}"
					)
				rc = out_config["t2"]['alias'][rc]

			if isinstance(rc, dict):
				override = t2.get('override', None)
				if override: 
					rc = {**rc, **override}
				# out_config['t2']['runConfig'] is an instance of T2RunConfigCollector
				t2['runConfig'] = out_config['t2']['runConfig'].add(rc)
				continue

			if isinstance(rc, int):
				if rc in out_config['t2']['runConfig']:
					continue
				raise ValueError(
					f"Unknown T2 run config alias defined in channel {k}:\n {t2}"
				)

			raise ValueError(
				f"Invalid T2 run config defined in process {self.process['processName']}"
			)


	def hash_t2_run_config(self, out_config: BaseConfig) -> 'ProcessMorpher':
		"""
		"""

		if self.process['tier'] not in (0, 1):
			return self

		ConfigUtils.walk_and_process_dict(
			arg=self.process, 
			callback=self._hash_t2_run_config_callback,
			match=("t2Compute"),
			out_config=out_config
		)

		return self
