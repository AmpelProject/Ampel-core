#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ProcessMorpher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 08.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Dict, Any, Literal
from importlib import import_module
from pydantic import create_model
from pydantic.main import ModelMetaclass
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
from ampel.model.StrictModel import StrictModel
from ampel.model.ProcessModel import ProcessModel
from ampel.model.Secret import Secret
from ampel.model.UnitModel import UnitModel
from ampel.base.DataUnit import DataUnit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.util.mappings import walk_and_process_dict
from ampel.util.type_analysis import get_subtype


class ProcessMorpher:
	""" Applies various transformations to process dicts """

	def __init__(self,
		process: Dict[str, Any],
		templates: Dict[str, Any],
		logger: AmpelLogger,
		verbose: bool = False,
		deep_copy: bool = False
	) -> None:

		self.process = json.loads(json.dumps(process)) if deep_copy else process
		self.templates = templates
		self.logger = logger
		self.verbose = verbose


	def get(self) -> Dict[str, Any]:
		""" :raises: Error if the morphed process does not comply with ProcessModel """
		return ProcessModel(**self.process).dict()


	def enforce_t3_channel_selection(self, chan_name: str) -> 'ProcessMorpher':

		if self.process['tier'] == 3:

			for directive in self.process['processor']['config']['directives']:
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


	def apply_template(self) -> 'ProcessMorpher':
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
			](**self.process).get_process(self.logger)

		return self


	def scope_aliases(self, first_pass_config: Dict) -> 'ProcessMorpher':

		if self.process.get('distrib'):

			walk_and_process_dict(
				arg = self.process,
				callback = self._scope_alias_callback,
				match = ['config'],
				first_pass_config = first_pass_config
			)

		else:

			self.logger.error(
				f'Cannot scope aliases for process {self.process["name"]}'
				f' as it is missing a distribution name'
			)

		return self


	def hash_t2_config(self, out_config: Dict) -> 'ProcessMorpher':

		if self.process['tier'] not in (0, 1):
			return self

		walk_and_process_dict(
			arg = self.process,
			callback = self._hash_t2_config_callback,
			match = ['t2_compute'],
			out_config = out_config
		)

		return self


	@classmethod
	def _create_model(cls, name, annotations, defaults, exclude):
		"""
		Build a pydantic model from annotations and defaults, replacing Secret
		fields with their dict representations
		"""
		fields = {
			k: (v, defaults[k] if k in defaults else ...)
			for k, v in annotations.items() if k not in exclude
		} # type: ignore
		# special case for Secret fields
		for k in list(fields.keys()):
			field_type = fields[k][0]
			if get_subtype(Secret, field_type):
				field_type = Dict[Literal["key"],str]
				if get_subtype(type(None), field_type):
					field_type = Optional[field_type]
				fields[k] = (field_type,) + fields[k][1:]
			elif type(field_type) is ModelMetaclass:
				fields[k] = (
					cls._create_model(
						field_type.__name__,
						field_type.__annotations__,
						field_type.__field_defaults__,
						set()
					),
					field_type.__field_defaults__
				)
		return create_model(
			name, __config__ = StrictModel.__config__,
			**fields
		)


	# TODO: verbose print path ?
	def _hash_t2_config_callback(self, path, k, d, **kwargs) -> None:

		out_config = kwargs.get('out_config')

		if not out_config:
			raise ValueError('Parameter "out_config" missing in kwargs')

		for t2 in d[k]['units']:

			if not isinstance(t2, dict):
				raise ValueError(f'Illegal unit definition: {t2}')
			else:
				# Trigger config validation (if enabled in UnitModel)
				UnitModel(**t2)

			rc = t2.get('config', None)
			t2_unit_name = t2['unit']

			if t2_unit_name not in out_config['unit']['base']:
				raise ValueError(f"Unknown T2 unit: {t2_unit_name}")

			if not rc:
				continue

			# alias
			if isinstance(rc, str):
				if rc not in out_config['alias']['t2']:
					raise ValueError(
						f'Unknown T2 config alias ({rc}) defined in process {self.process["name"]}'
					)
				rc = out_config['alias']['t2'][rc]

			if isinstance(rc, dict):

				if override := t2.get('override', None):
					rc = {**rc, **override}

				if fqn := out_config['unit']['base'][t2_unit_name].get('fqn'):

					T2Unit = getattr(import_module(fqn), fqn.split('.')[-1])
					excl: Any = DataUnit._annots.keys()
					if issubclass(T2Unit, AbsPointT2Unit):
						excl = list(excl) + ["ingest"]
					model = self._create_model(
						t2_unit_name,
						T2Unit._annots,
						T2Unit._defaults,
						excl,
					)

					rc = model(**rc).dict()

				else:
					self.logger.warn(
						f"T2 unit {t2_unit_name} not installed locally. "
						f"Building *unsafe* conf dict hash: "
						f"changes in unit defaults between releases will go undetected"
					)

				# out_config['confid'] is an instance of T02ConfigCollector
				t2['config'] = out_config['confid'].add(rc)
				continue

			# For internal use only
			if isinstance(rc, int):
				if rc in out_config['confid']:
					continue
				raise ValueError(
					f'Unknown T2 config (int) alias defined in channel {k}:\n {t2}'
				)

			raise ValueError(
				f'Invalid T2 config defined in process {self.process["name"]}'
			)


	# TODO: verbose print path ?
	def _scope_alias_callback(self, path, k, d, **kwargs):

		if 'first_pass_config' not in kwargs:
			raise ValueError('Parameter "first_pass_config" missing in kwargs')

		v = d[k]

		if v and isinstance(v, str):

			if v[0] == '%':
				scoped_alias = v[1:]
			else:
				scoped_alias = f'{self.process["distrib"]}/{v}'

			if not any([
				scoped_alias in kwargs['first_pass_config']['alias'][f't{tier}']
				for tier in (0, 1, 2, 3)
			]):
				raise ValueError(f'Alias "{scoped_alias}" not found')

			# Overwrite
			d[k] = scoped_alias

			if self.verbose:
				self.logger.log(VERBOSE, f'Alias "{v}" renamed into "{scoped_alias}"')
