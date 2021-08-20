#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/UnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 21.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys
from importlib import import_module
from functools import partial
from pathlib import Path
from hashlib import blake2b
from contextlib import contextmanager
from typing import Dict, Iterator, Type, Any, Union, Optional, \
	TypeVar, Sequence, List, overload, cast, get_args
from pydantic.main import create_model
from ampel.types import check_class
from ampel.util.collections import ampel_iter
from ampel.util.freeze import recursive_unfreeze
from ampel.util.mappings import merge_dicts
from ampel.view.ReadOnlyDict import ReadOnlyDict
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.base.LogicalUnit import LogicalUnit
from ampel.core.AmpelContext import AmpelContext
from ampel.core.ContextUnit import ContextUnit
from ampel.core.AmpelDB import AmpelDB
from ampel.model.StrictModel import StrictModel
from ampel.model.UnitModel import UnitModel
from ampel.abstract.Secret import Secret
from ampel.secret.AmpelVault import AmpelVault
from ampel.model.t3.AliasableModel import AliasableModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import dictify

T = TypeVar('T', bound=AmpelBaseModel)
LT = TypeVar('LT', bound=LogicalUnit)
CT = TypeVar('CT', bound=ContextUnit)
pyv = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


class UnitLoader:

	def __init__(self,
		config: AmpelConfig,
		db: Optional[AmpelDB],
		provenance: bool = True,
		vault: Optional[AmpelVault] = None
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
		self.unit_defs: Dict = config._config['unit']
		self.aliases: List[Dict] = [config._config['alias'][f"t{el}"] for el in (0, 3, 1, 2)]
		self._dyn_register: Optional[Dict[str, Type[LogicalUnit]]] = None # potentially updated by DevAmpelContext



	@overload
	def new_logical_unit(self,
		model: UnitModel, logger: AmpelLogger, *, sub_type: Type[LT], **kwargs
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
		sub_type: Optional[Type[LT]] = None,
		**kwargs
	) -> Union[LT, LogicalUnit]:
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


	@overload
	def new_context_unit(self,
		model: UnitModel, context: AmpelContext, *, sub_type: Type[CT], **kwargs
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
		sub_type: Optional[Type[CT]] = None,
		**kwargs
	) -> Union[CT, ContextUnit]:
		"""
		Context units require an AmpelContext instance as init parameters, additionaly to
		potentialy defined dedicated custom parameters.
		:raises: ValueError is the unit defined in the model is unknown
		"""
		return self.new(
			model, unit_type=sub_type or ContextUnit, context=context, **kwargs
		)


	@overload
	def new(self, model: UnitModel, *, unit_type: Type[T], **kwargs) -> T:
		...
	@overload
	def new(self, model: UnitModel, *, unit_type: None = ..., **kwargs) -> AmpelBaseModel:
		...
	def new(self,
		model: UnitModel, *,
		unit_type: Optional[Type[T]] = None,
		**kwargs
	) -> Union[AmpelBaseModel, T]:
		"""
		Instantiate new object based on provided model and kwargs.
		:param 'unit_type': performs isinstance check and raise error on mismatch. Enables mypy/other static checks.
		:returns: unit instance, trace id (0 if not computable)
		"""

		if not isinstance(model, UnitModel):
			raise ValueError(f"Unexpected model: '{type(model)}'")

		provenance = kwargs.pop('_provenance', self.provenance)

		Klass = self.get_class_by_name(model.unit, unit_type) # type: ignore
		if unit_type:
			check_class(Klass, unit_type)

		init_config = self.get_init_config(model.config, model.override)
		unit = Klass(**(init_config | kwargs | (model.secrets or {})))
		trace_id = 0

		# potentially sync trace_ids with DB (Ampel_ext)
		if provenance and '_trace_content' in unit.__dict__:

			assert self.db

			trace_dict = {
				'py': pyv,
				'unit': model.unit,
				'digest': self.get_digest(Klass),
				'version': self.config.get(f"unit.{model.unit}.version", str, raise_exc=True)
			}

			if c := unit.__dict__.get("_trace_content"):
				trace_dict['config'] = dictify(c)

			if env := self.config.get(f"unit.{model.unit}.env", dict):
				trace_dict['env'] = env

			try:

				# Note: we could implement a hash collision detection mechanism here
				trace_id = build_unsafe_dict_id(trace_dict, ret=int)

				# Save trace id to external collection
				if trace_id not in self.db.trace_ids:
					trace_dict['_id'] = trace_id
					self.db.add_trace_id(trace_id, trace_dict)

			# Non-serializable content
			except Exception:
				trace_id = -1
				# raise e

		unit._trace_id = trace_id # type: ignore[union-attr]

		# Resolve secrets
		for k, v in unit.__dict__.items():
			if isinstance(v, Secret):
				ValueType = args[0] if (args := get_args(type(unit).__annotations__[k])) else object
				if not self.vault:
					raise ValueError("No vault configured")
				if not self.vault.resolve_secret(v, ValueType):
					raise ValueError(f"Secret[{ValueType.__name__}] {k} not found")
				
		if hasattr(unit, "post_init"):
			unit.post_init() # type: ignore[union-attr]

		return unit


	@staticmethod
	def get_digest(Klass: Type) -> str:

		try:
			return blake2b(
				Path(sys.modules[Klass.__module__].__file__).read_bytes()
			).hexdigest()[:7]
		except Exception:
			return "unspecified"


	@overload
	def get_class_by_name(self, name: str, unit_type: Type[T]) -> Type[T]:
		...
	@overload
	def get_class_by_name(self, name: str, unit_type: None = ...) -> Type:
		...
	def get_class_by_name(self, name: str, unit_type: Optional[Type[T]] = None) -> Union[Type, Type[T]]:
		"""
		Matches the parameter 'name' with the unit definitions defined in the ampel_config.
		This allows to retrieve the corresponding fully qualified name of the class and to load it.

		:param unit_type:
			- LogicalUnit or any sublcass of LogicalUnit
			- ContextUnit or any sublcass of ContextUnit
			- If None (auxiliary class), returned object will have Type[Any]

		:raises: ValueError if unit cannot be found or loaded or if parent class is unrecognized
		"""
		if name in AuxUnitRegister._defs:
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
		config: Optional[Union[int, str, Dict[str, Any]]] = None,
		override: Optional[Dict[str, Any]] = None,
		kwargs: Optional[Dict[str, Any]] = None,
		unfreeze: bool = True
	) -> Dict[str, Any]:
		""" :raises: ValueError if config alias is not found """

		ret: Optional[Dict[str, Any]] = {}

		if isinstance(config, (dict, str)):
			ret = self.resolve_aliases(config)

		elif isinstance(config, int):

			try:
				d = self.config.get_conf_id(config)
			# confid not found (obsolete or dynamically generated by isolated process)
			except Exception as e:
				assert self.db
				l = list(self.db.col_conf_ids.find({"_id": config}))
				if len(l) == 0:
					raise e
				del l[0]['_id']
				d = l[0]
				
			ret = recursive_unfreeze(d) if unfreeze and isinstance(d, ReadOnlyDict) else d

			# save un-registered (in ampel conf but not in db) confid to external collection for posterity
			if self.provenance:
				assert self.db
				if config not in self.db.conf_ids:
					self.db.add_conf_id(config, ret)

		if ret is None and config is not None:
			raise ValueError(f"Config alias {config} not found")

		return merge_dicts([ret, override, kwargs]) or {}


	def resolve_aliases(self, value):
		"""
		Recursively resolve aliases from config
		"""
		if isinstance(value, str):
			for adict in self.aliases:
				if value in adict:
					return self.resolve_aliases(adict[value])
			return value
		elif isinstance(value, list):
			return [self.resolve_aliases(v) for v in value]
		elif isinstance(value, dict):
			return {k: self.resolve_aliases(v) for k, v in value.items()}
		else:
			return value


	def get_resources(self, model: UnitModel) -> Dict[str, Any]:
		"""
		Resources are defined using the static variable 'require' in ampel units
		-> example: catsHTM.default
		"""

		resources: Dict[str, Any] = {}
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
		"""
		Enable validation for UnitModel instances
		"""
		extra_validator = (False, partial(_validate_unit_model, unit_loader=self))
		UnitModel.__post_root_validators__.append(extra_validator)
		AliasableModel._config = self.config
		try:
			yield
		finally:
			UnitModel.__post_root_validators__.remove(extra_validator)
			AliasableModel._config = None


	"""
	def internal_mypy_tests_uncomment_only_in_your_editor(self,
		model: UnitModel, context: AmpelContext, logger: AmpelLogger, sub_type: Optional[Type[CT]] = None, **kwargs
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

def _validate_unit_model(cls, values: Dict[str, Any], unit_loader: UnitLoader) -> Dict[str, Any]:
	"""
	Verify that a unit configuration is valid in the context of a specific UnitLoader.
	"""
	from ampel.base.LogicalUnit import LogicalUnit
	from ampel.core.ContextUnit import ContextUnit
	from ampel.abstract.AbsEventUnit import AbsEventUnit
	from ampel.abstract.AbsDocIngester import AbsDocIngester
	from ampel.abstract.AbsT3Stager import AbsT3Stager
	from ampel.abstract.AbsSessionInfo import AbsSessionInfo

	unit = unit_loader.get_class_by_name(values['unit'])
	if issubclass(unit, (LogicalUnit, ContextUnit, AbsEventUnit, AbsDocIngester)):
		# exclude base class fields provided at runtime
		exclude = {"logger"}
		for parent in cast(
			Sequence[Type[AmpelBaseModel]],
			(LogicalUnit, ContextUnit, AbsT3Stager, AbsSessionInfo, AbsEventUnit, AbsDocIngester)
		):
			if issubclass(unit, parent):
				exclude.update(set(parent._annots.keys()).difference(parent._defaults.keys()))
		fields = {
			k: (v, unit._defaults[k] if k in unit._defaults else ...)
			for k, v in unit._annots.items() if k not in exclude
		} # type: ignore
		model: Any = create_model(
			unit.__name__, __config__ = StrictModel.__config__,
			__base__=None, __module__=None, __validators__=None, # type: ignore
			**fields
		)
		model.validate(
			unit_loader.get_init_config(
				values['config'], values['override']
			)
		)
	return values
