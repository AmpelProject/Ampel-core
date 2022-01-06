#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ProcessMorpher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 16.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Any
from importlib import import_module
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
from ampel.model.ProcessModel import ProcessModel
from ampel.util.recursion import walk_and_process_dict
from ampel.util.mappings import dictify
from ampel.util.hash import build_unsafe_dict_id
from ampel.base.LogicalUnit import LogicalUnit


class ProcessMorpher:
	""" Applies various transformations to process dicts """

	def __init__(self,
		process: dict[str, Any],
		templates: dict[str, Any],
		logger: AmpelLogger,
		verbose: bool = False,
		deep_copy: bool = False
	) -> None:

		self.process = json.loads(json.dumps(process)) if deep_copy else process
		self.templates = templates
		self.logger = logger
		self.verbose = verbose


	def get(self) -> dict[str, Any]:
		""" :raises: Error if the morphed process does not comply with ProcessModel """
		return ProcessModel(**self.process).dict()


	def generate_version(self, first_pass_config: dict) -> 'ProcessMorpher':
		"""
		Generates a version identifier for the underyling process as required by ProcessModel

		:param first_pass_config: required to get the version identifier of the ampel units used by this process
		"""
		
		# TODO: implement
		if not self.process.get("version"):
			self.process['version'] = build_unsafe_dict_id(
				dictify(self.process['processor'].get("config", {}))
			)

		return self


	def enforce_t3_channel_selection(self, chan_name: str) -> 'ProcessMorpher':

		if self.process['tier'] == 3:

			directive = self.process['processor']['config']['supply']
			if 'select' in directive:
				select = directive['select']
				if select['unit'] not in ('T3StockSelector', 'T3FilteringStockSelector'):
					raise ValueError(
						f'Cannot enforce channel selection: '
						f'unknown stock selection unit {select["unit"]}'
					)
			else:
				select = {'unit': 'T3StockSelector', 'config': {}}
				directive['select'] = select

			if 'config' not in select:
				select['config'] = {}

			if self.verbose:
				action = 'Modifying' if 'channel' in select['config'] else 'Adding'
				self.logger.log(VERBOSE,
					f'{action} channel selection criteria ({chan_name}) '
					f'for process {self.process["name"]}'
				)

			# processes embedded in channel must feature a transient selection.
			# An exception will be raised if someone embeds an admin t3 proc (without selection)
			# within a channel (unsupported feature)
			select['config']['channel'] = chan_name

		return self


	def apply_template(self, first_pass_config: dict) -> 'ProcessMorpher':
		""" Applies template possibly associated with process """

		# The process embedded in channel def requires templating itself
		if 'template' in self.process:

			if self.verbose:
				self.logger.log(VERBOSE,
					f'Applying template {self.process["template"]} '
					f'to process {self.process["name"]} '
					f'from distribution {self.process["distrib"]}'
				)

			self.process = self.templates[
				self.process['template']
			](**self.process).get_process(first_pass_config, self.logger)

		return self


	def scope_aliases(self, first_pass_config: dict) -> 'ProcessMorpher':
		""" Note: should be called before hash_t2_config """

		if self.process.get('distrib'):

			if self.verbose:
				self.logger.debug("Scoping aliases")

			recurse = False

			while walk_and_process_dict(
				arg = self.process,
				callback = self._scope_alias_callback,
				match = ['config'],
				first_pass_config = first_pass_config
			):
				if self.verbose:
					self.logger.debug("Alias(es) scoped %s" % ("again" if recurse else ""))
				recurse = True
		else:

			self.logger.error(
				f'Cannot scope aliases for process {self.process["name"]}'
				f' as it is missing a distribution name'
			)

		return self


	def _scope_alias_callback(self, path, k, d, **kwargs) -> bool:
		"""
		:returns: true if modification was done
		"""

		if 'first_pass_config' not in kwargs:
			raise ValueError('Parameter "first_pass_config" missing in kwargs')

		v = d[k]

		if v and isinstance(v, str):

			# Global alias
			if v[0] == '%':
				return False

			# Alias already scoped
			if "/" in v:
				return False

			scoped_alias = f'{self.process["distrib"]}/{v}'

			if not any([
				scoped_alias in kwargs['first_pass_config']['alias'][f't{tier}']
				for tier in (0, 1, 2, 3)
			]):
				raise ValueError(f'Alias "{scoped_alias}" not found')

			# Overwrite
			d[k] = scoped_alias

			if self.verbose:
				self.logger.log(VERBOSE, f'Config alias "{v}" renamed into "{scoped_alias}"')

			return True

		return False


	def resolve_aliases(self, t2d: dict[str, Any], aliases: dict[str, Any], root_path: str):
		"""
		Resolves aliases recursively (necessary before hashing t2 config)
		"""

		if self.verbose:
			self.logger.debug("Resolving aliases (required for hash)")

		recurse = False

		while walk_and_process_dict(
			arg = t2d,
			callback = self._resolve_alias_callback,
			match = ['config'],
			aliases = aliases,
			root_path = root_path
		):
			if self.verbose:
				self.logger.debug("Alias(es) resolved %s" % ("again" if recurse else ""))
			recurse = True


	def _resolve_alias_callback(self, path, k, d, **kwargs) -> None:
		"""
		Used by walk_and_process_dict(...) from resolve_aliases(...)
		"""

		if not isinstance(d[k], str):
			return

		aliases = kwargs.get('aliases')

		if not aliases:
			raise ValueError('Parameter "aliases" missing in kwargs')

		if d[k] not in aliases:
			raise ValueError(
				f'Unknown T2 config alias ({d[k]}) defined in process {self.process["name"]}'
			)

		if self.verbose:
			self.logger.debug(f"Resolving alias '{d[k]}' in {kwargs.get('root_path')}")

		d[k] = aliases[d[k]]


	def hash_t2_config(self, out_config: dict) -> 'ProcessMorpher':
		"""
		This method modifies the underlying self._process dict structure.
		The 'config' (dict) value of UnitModel instances is replaced by a hash value (int).
		Notes:
		- Applies only to t0 and t1 processes
		- The config path filter "t2_compute.units" is used
		- Aliases are resolved (works recursively so that t2_dependency of tied units can use aliases as well)
		
		:param out_config: used to store new map entries in the ampel config: {<confid>: {<hash>: <conf dict>}}.
		"""

		if self.process['tier'] not in (0, 1):
			return self

		if self.verbose:
			self.logger.debug("Looking for t2 config to hash")

		# Example of conf_dicts keys
		# processor.config.directives.0.combine.0.state_t2.0
 		# processor.config.directives.0.combine.0.state_t2.0.config.t2_dependency.0
 		# processor.config.directives.0.point_t2.0
		conf_dicts: dict[str, dict[str, Any]] = {}

		walk_and_process_dict(
			arg = self.process,
			callback = self._gather_t2_config_callback,
			match = ['point_t2', 'stock_t2', 'state_t2'],
			conf_dicts = conf_dicts
		)

		# This does the trick of processing nested config first
		sorted_conf_dicts = {
			k: conf_dicts[k]
			for k in sorted(conf_dicts, key=len, reverse=True)
		}

		for k, d in sorted_conf_dicts.items():

			t2_unit = d["unit"]
			conf = d.get("config", {})

			if self.verbose:
				extra = {'process': self.process['name'], 'conf': k + ".config"}
				self.logger.debug("Hashing T2 config", extra=extra)

			# Replace alias with content
			if isinstance(conf, str):
			
				if conf not in out_config['alias']['t2']:
					raise ValueError(
						f'Unknown T2 config alias ({conf}) defined in process {self.process["name"]}'
					)

				if self.verbose:
					self.logger.debug(f"Resolving alias '{conf}'", extra=extra)

				conf = out_config['alias']['t2'][conf]

			if isinstance(conf, dict):

				if override := d.get("override"):
					conf = {**conf, **override}

				if fqn := out_config['unit'][t2_unit].get('fqn'):

					T2Unit: LogicalUnit = getattr(import_module(fqn), fqn.split('.')[-1])
					conf = T2Unit.validate(conf)

				else:

					self.logger.warn(
						f"T2 unit {t2_unit} not installed locally. "
						f"Building *unsafe* conf dict hash: "
						f"changes in unit defaults between releases will go undetected",
						extra=extra
					)

				if self.verbose:
					self.logger.debug("Computing hash", extra=extra)

				if conf is None:
					d["config"] = None
				else:
					d["config"] = out_config['confid'].add(conf, None, None, None)

			# For internal use only
			elif isinstance(conf, int):
				if conf not in out_config['confid']:
					raise ValueError(
						f'Unknown T2 config (int) alias defined by {k}:\n {t2_unit}'
					)
			else:
				raise ValueError(
					f'Unknown T2 config defined by {k}:\n {t2_unit}'
				)

		return self


	def _gather_t2_config_callback(self, path, k, d, **kwargs) -> None:
		"""
		Used by walk_and_process_dict(...) from hash_t2_config(...)
		"""

		if self.verbose:
			self.logger.info(f"# path: {path}.{k}")

		if d[k]:
			for i, el in enumerate(d[k]):
				if self.verbose:
					self.logger.info(f"# path: {path}.{k}.{i}")
					self.logger.info(el)
				kwargs['conf_dicts'][f"{path}.{k}.{i}"] = el
