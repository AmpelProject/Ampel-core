#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/UnitLoader.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                07.10.2019
# Last Modified Date:  11.11.2025
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import contextlib
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import blake2b
from importlib import import_module
from pathlib import Path
from typing import Any, TypeVar, overload

from ampel.base.AmpelUnit import AmpelUnit
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.base.LogicalUnit import LogicalUnit
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AmpelDB import AmpelDB
from ampel.core.ContextUnit import ContextUnit
from ampel.abstract.AbsUnitResultAdapter import AbsUnitResultAdapter
from ampel.log.AmpelLogger import VERBOSE, AmpelLogger, LogFlag
from ampel.log.handlers.ChanRecordBufHandler import ChanRecordBufHandler
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.model.t3.AliasableModel import AliasableModel
from ampel.model.UnitModel import UnitModel
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.NamedSecret import NamedSecret
from ampel.types import ChannelId, check_class
from ampel.util.collections import ampel_iter
from ampel.util.freeze import recursive_unfreeze
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import merge_dicts
from ampel.view.ReadOnlyDict import ReadOnlyDict

T = TypeVar('T', bound=AmpelUnit)
LT = TypeVar('LT', bound=LogicalUnit)
CT = TypeVar('CT', bound=ContextUnit)
pyv = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
env = ('conda_' + os.environ["CONDA_DEFAULT_ENV"]) if 'CONDA_DEFAULT_ENV' in os.environ else 'default'

class UnitLoader:

	def __init__(self,
		config: AmpelConfig,
		db: None | AmpelDB,
		provenance: bool = True,
		vault: None | AmpelVault = None
	) -> None:
		"""
		:raises: ValueError in case bad arguments are provided
		"""

		if not isinstance(config, AmpelConfig):
			raise ValueError(
				f"First parameter must be an instance of "
				f"AmpelConfig (provided: {type(config)})"
			)
		
		if provenance and not db:
			raise ValueError("Provenance tracking requires a database connection")

		self.db = db
		self.vault = vault
		self.config = config
		self.provenance = provenance
		self.unit_defs: dict = config._config['unit']  # noqa: SLF001
		self.aliases: list[dict] = [config._config['alias'][f"t{el}"] for el in (0, 3, 1, 2)]  # noqa: SLF001

		# potentially updated by DevAmpelContext
		self._dyn_register: dict[str, type[LogicalUnit] | type[ContextUnit]] | None = None
		self._adapters: dict[str, AbsUnitResultAdapter] = {}


	@overload
	def new_logical_unit(self,
		model: UnitModel, logger: AmpelLogger, *, sub_type: type[LT], **kwargs
	) -> LT:
		...
	@overload
	def new_logical_unit(self,
		model: UnitModel, logger: AmpelLogger, *, sub_type: None = ..., **kwargs
	) -> LogicalUnit:
		...
	def new_logical_unit(self,
		model: UnitModel,
		logger: AmpelLogger, *,
		sub_type: None | type[LT] = None,
		**kwargs
	) -> LT | LogicalUnit:
		"""
		Logical units require logger and resource as init parameters, additionaly to the potentialy
		defined custom parameters which will be provided as a union of the model config
		and the kwargs provided to this method (the latter having prevalance)
		:raises: ValueError is the unit defined in the model is unknown
		"""
		return self.new(
			model,
			unit_type = sub_type or LogicalUnit,
			logger = logger,
			resource = self.get_resources(model),
			**kwargs
		)


	def new_safe_logical_unit(self,
		um: UnitModel,
		unit_type: type[LT],
		logger: AmpelLogger,
		_chan: None | ChannelId = None
	) -> LT:
		""" Returns a logical unit with dedicated logger containing no db handler """

		if logger.verbose:
			logger.log(VERBOSE, f"Instantiating unit {um.unit}")

		buf_hdlr = ChanRecordBufHandler(logger.level, _chan, {'unit': um.unit}) if _chan \
			else DefaultRecordBufferingHandler(logger.level, {'unit': um.unit})

		# Spawn unit instance
		unit = self.new_logical_unit(
			model = um,
			logger = AmpelLogger(
				base_flag = (getattr(logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = len(logger.handlers) == 1, # to be improved later
				handlers = [buf_hdlr]
			),
			sub_type = unit_type
		)

		setattr(unit, '_buf_hdlr', buf_hdlr) # noqa: B010
		return unit


	@overload
	def new_context_unit(self,
		model: UnitModel, context: AmpelContext, *, sub_type: type[CT], **kwargs
	) -> CT:
		...
	@overload
	def new_context_unit(self,
		model: UnitModel, context: AmpelContext, *, sub_type: None = ..., **kwargs
	) -> ContextUnit:
		...
	def new_context_unit(self,
		model: UnitModel,
		context: AmpelContext, *,
		sub_type: None | type[CT] = None,
		**kwargs
	) -> CT | ContextUnit:
		"""
		Context units require an AmpelContext instance as init parameters, additionaly to
		potentialy defined dedicated custom parameters.
		:raises: ValueError is the unit defined in the model is unknown
		"""
		return self.new(
			model, unit_type=sub_type or ContextUnit, context=context, **kwargs
		)


	def get_adapter(self, context: AmpelContext, model: UnitModel, run_id: int) -> AbsUnitResultAdapter:
		akey = f"{context.uuid}_{run_id}_{build_unsafe_dict_id(model.dict())}"
		if (akey not in self._adapters):
			self._adapters[akey] = self.new(
				model, unit_type=AbsUnitResultAdapter, context=context, run_id = run_id
			)

		return self._adapters[akey]


	def clear_adapter_cache(self) -> None:
		self._adapters.clear()


	@overload
	def new(self, model: UnitModel, *, unit_type: type[T], **kwargs) -> T:
		...
	@overload
	def new(self, model: UnitModel, *, unit_type: None = ..., **kwargs) -> AmpelUnit:
		...
	def new(self, model: UnitModel, *, unit_type: None | type[T] = None, **kwargs) -> AmpelUnit | T:
		"""
		Instantiate new object based on provided model and kwargs.
		:param 'unit_type': performs isinstance check and raise error on mismatch. Enables mypy/other static checks.
		:returns: unit instance, trace id (0 if not computable)
		"""

		if not isinstance(model, UnitModel):
			raise ValueError(f"Unexpected model: '{type(model)}'")

		provenance = kwargs.pop('_provenance', self.provenance)

		Klass = self.get_class_by_name(model.unit, unit_type)
		if unit_type:
			check_class(Klass, unit_type)

		with NamedSecret.resolve_with(self.vault):
			unit = Klass(**(
				self.get_init_config(model.config, model.override)
				| kwargs
				| (model.secrets or {})
			))

			if hasattr(unit, "post_init"):
				unit.post_init()
		
		if isinstance(unit, LogicalUnit | ContextUnit):

			trace_id = None

			# potentially sync trace_ids with DB (Ampel_ext)
			if provenance:

				assert self.db

				trace_dict = {
					'py': pyv,
					'unit': model.unit,
					'digest': self.get_digest(Klass),
					'version': self.config.get(f"unit.{model.unit}.version", str, raise_exc=True)
				}

				if c := unit._get_trace_content():  # noqa: SLF001
					trace_dict['config'] = c

				if deps := self.config.get(f"unit.{model.unit}.dependencies"):
					if not isinstance(deps, list | tuple):
						raise ValueError(f"Retrieved environment is not a list/tuple: {type(deps)}")
					envd = self.config.get(f"environment.{env}", dict, raise_exc=True)
					trace_dict['env'] = {k: envd[k] for k in deps}

				try:

					# Note: we could implement a hash collision detection mechanism here
					trace_id = build_unsafe_dict_id(trace_dict, ret=int)

					# Save trace id to external collection
					if trace_id not in self.db.trace_ids:
						trace_dict['_id'] = trace_id
						self.db.add_trace_id(trace_id, trace_dict)

				# Non-serializable content
				except Exception:
					trace_id = 0
					# raise e

			unit._trace_id = trace_id  # noqa: SLF001

		return unit


	@staticmethod
	def get_digest(Klass: type) -> str:

		try:
			return blake2b(
				Path(sys.modules[Klass.__module__].__file__).read_bytes() # type: ignore[arg-type]
			).hexdigest()[:7]
		except Exception:
			return "unspecified"


	@overload
	def get_class_by_name(self, name: str, unit_type: type[T]) -> type[T]:
		...
	@overload
	def get_class_by_name(self, name: str, unit_type: None = ...) -> type[AmpelUnit]:
		...
	def get_class_by_name(self, name: str, unit_type: None | type[T] = None) -> type[T | AmpelUnit]:
		"""
		Matches the parameter 'name' with the unit definitions defined in the ampel_config.
		This allows to retrieve the corresponding fully qualified name of the class and to load it.

		:param unit_type:
			- LogicalUnit or any sublcass of LogicalUnit
			- ContextUnit or any sublcass of ContextUnit
			- If None (auxiliary class), returned object will have type[Any]

		:raises: ValueError if unit cannot be found or loaded or if parent class is unrecognized
		"""
		with contextlib.suppress(ValueError):
			return AuxUnitRegister.get_aux_class(name, sub_type=unit_type)

		if self._dyn_register and name in self._dyn_register:
			return self._dyn_register[name]

		if name in self.unit_defs:
			fqn = self.unit_defs[name]['fqn']
		else:
			raise ValueError(f"Ampel unit not found: {name}")

		# Note: importlib.import_module caches internally imported modules
		return getattr(import_module(fqn), name)


	def get_init_config(self,
		config: None | int | str | dict[str, Any] = None,
		override: None | dict[str, Any] = None,
		kwargs: None | dict[str, Any] = None,
		unfreeze: bool = True
	) -> dict[str, Any]:
		""" :raises: ValueError if config alias is not found """

		base_conf: dict[str, Any] = {}

		if isinstance(config, dict | str):

			base_conf = self.resolve_aliases(config)
			if base_conf is None:
				raise ValueError(f"Config alias {config} not found")

		# Hashed t2 unit configs
		elif isinstance(config, int):

			try:
				d = self.config.get_conf_by_id(config)
			# confid not found (obsolete or dynamically generated by isolated process)
			except Exception as e:
				assert self.db
				l = list(self.db.col_conf_ids.find({"_id": config}))
				if len(l) == 0:
					raise e
				del l[0]['_id']
				d = l[0]
				
			base_conf = recursive_unfreeze(d) if (unfreeze and isinstance(d, ReadOnlyDict)) else d

			# save un-registered (in ampel conf but not in db) confid to external collection for posterity
			if self.provenance:
				assert self.db
				if config not in self.db.conf_ids:
					self.db.add_conf_id(config, base_conf)

		return merge_dicts([base_conf, override, kwargs]) or {}


	def resolve_aliases(self, value):
		"""
		Recursively resolve aliases from config
		"""
		if isinstance(value, str):
			for adict in self.aliases:
				if value in adict:
					return self.resolve_aliases(adict[value])
			return value
		if isinstance(value, list):
			return [self.resolve_aliases(v) for v in value]
		if isinstance(value, dict):
			return {k: self.resolve_aliases(v) for k, v in value.items()}

		return value


	def get_resources(self, model: UnitModel) -> dict[str, Any]:
		"""
		Resources are defined using the static variable 'require' in ampel units
		-> example: catsHTM.default
		"""

		resources: dict[str, Any] = {}
		Klass = self.get_class_by_name(model.unit)

		# Load possibly required global resources
		for k in ampel_iter(getattr(Klass, 'require', [])):

			if k is None:
				continue

			# Global resource example: extcat
			if (resource := self.config.get(f'resource.{k}')) is None:
				raise ValueError(f"Global resource not available: {k}")

			resources[k] = resource

		return resources


	@contextmanager
	def validate_unit_models(self) -> Iterator[None]:
		""" Enable validation for UnitModel instances """

		@staticmethod # type: ignore[misc]
		def validate_unit(value: UnitModel) -> UnitModel:
			Unit = self.get_class_by_name(value.unit)
			if issubclass(Unit, AmpelUnit):
				Unit.validate(self.get_init_config(value.config, value.override))
			return value

		UnitModel.post_validate_hook = validate_unit
		AliasableModel._config = self.config  # noqa: SLF001
		try:
			with NamedSecret.resolve_with(self.vault):
				yield
		finally:
			UnitModel.post_validate_hook = None
			AliasableModel._config = None  # noqa: SLF001


	"""
	def internal_mypy_tests_uncomment_only_in_your_editor(self,
		model: UnitModel, context: AmpelContext, logger: AmpelLogger, sub_type: None | type[CT] = None, **kwargs
	) -> None:

		# Interal: uncomment to check if mypy works adequately
		from ampel.abstract.AbsEventUnit import AbsEventUnit
		from ampel.abstract.AbsLightCurveT2Unit import AbsLightCurveT2Unit

		reveal_type(self.new(model))
		reveal_type(self.new(model, bla=12))
		reveal_type(self.new(model, unit_type = None))
		reveal_type(self.new(model, unit_type=AbsLightCurveT2Unit))
		reveal_type(self.new(model, unit_type=AbsLightCurveT2Unit, bla=12))
		reveal_type(self.new(model, unit_type=AbsEventUnit))
		reveal_type(self.new(model, unit_type=AbsEventUnit, bla=12))

		reveal_type(self.new_logical_unit(model, logger))
		reveal_type(self.new_logical_unit(model, logger, bla=12))
		reveal_type(self.new_logical_unit(model, logger, sub_type = None))
		reveal_type(self.new_logical_unit(model, logger, sub_type=AbsLightCurveT2Unit))
		reveal_type(self.new_logical_unit(model, logger, sub_type = AbsLightCurveT2Unit, bla=12))

		# Next two lines *should* fail
		reveal_type(self.new_logical_unit(model, logger, sub_type=AbsEventUnit))
		reveal_type(self.new_logical_unit(model, logger, sub_type = AbsEventUnit, bla=12))

		reveal_type(self.new_context_unit(model, context))
		reveal_type(self.new_context_unit(model, context, bla=12))
		reveal_type(self.new_context_unit(model, context, sub_type = None))
		reveal_type(self.new_context_unit(model, context, sub_type = AbsEventUnit))
		reveal_type(self.new_context_unit(model, context, sub_type = AbsEventUnit, bla=12))

		# Next two lines *should* fail
		reveal_type(self.new_context_unit(model, context, sub_type = AbsLightCurveT2Unit))
		reveal_type(self.new_context_unit(model, context, sub_type = AbsLightCurveT2Unit, bla=12))
	"""
