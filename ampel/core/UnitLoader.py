#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/UnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 17.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from contextlib import contextmanager
from functools import partial
from importlib import import_module
from typing import (
	Dict, Iterator, Type, Any, Union, Optional, TypeVar, Sequence,
	Literal, List, overload, get_args, get_origin, cast
)

from pydantic.main import ModelMetaclass, create_model
from ampel.type import check_class
from ampel.util.collections import ampel_iter
from ampel.util.freeze import recursive_unfreeze
from ampel.util.mappings import merge_dicts
from ampel.util.type_analysis import get_subtype
from ampel.view.ReadOnlyDict import ReadOnlyDict
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.base.DataUnit import DataUnit
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AdminUnit import AdminUnit
from ampel.model.StrictModel import StrictModel
from ampel.model.UnitModel import UnitModel
from ampel.model.Secret import Secret
from ampel.model.t3.AliasableModel import AliasableModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsSecretProvider import AbsSecretProvider

T = TypeVar('T', bound=AmpelBaseModel)
BT = TypeVar('BT', bound=DataUnit)
PT = TypeVar('PT', bound=AdminUnit)


class UnitLoader:

	def __init__(self,
		config: AmpelConfig,
		tier: Optional[Literal[0, 1, 2, 3]] = None,
		secrets: Optional[AbsSecretProvider] = None,
		) -> None:
		"""
		For optimization purposes, try to set the parameter tier.
		For example, a T2 controller should spawn an UnitLoader
		using UnitLoader(ampel_config, 2).

		:raises: ValueError in case bad arguments are provided
		"""

		if not isinstance(config, AmpelConfig):
			raise ValueError(
				f"First parameter must be an instance of "
				f"AmpelConfig (provided: {type(config)})"
			)

		self.ampel_config = config
		self.unit_defs: List[Dict] = [
			config._config['unit']['controller'],
			config._config['unit']['base'],
			config._config['unit']['admin'],
			config._config["unit"]["core"],
			config._config["unit"]["aux"],
		]

		self.aliases: List[Dict] = [
			config._config['alias'][f"t{el}"] for el in (0, 3, 1, 2)
		]

		self.secrets = secrets


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

	@classmethod
	def resolve_secrets(
		cls,
		secrets: Optional[AbsSecretProvider],
		unit: Union[Type[AmpelBaseModel], ModelMetaclass],
		annotations: Dict[str, Any],
		defaults: Dict[str, Any],
		init_config: Dict[str, Any]
	) -> Dict[str, Any]:
		"""
		Recursively walk annotations, resolving any Secret fields that are found
		"""
		for field, typ in annotations.items():
			# Secret, or Union containing Secret
			if secret_field := get_subtype(Secret, typ):
				if field in init_config:
					value = init_config[field]
				elif field in defaults:
					value = defaults[field]
				else:
					raise KeyError(f"{unit.__qualname__}.{field} needs a value")

				# skip if optional or preconfigured
				if value is None and get_subtype(type(None), typ) or isinstance(value, Secret):
					continue
				elif not secrets:
					raise RuntimeError(f"{unit.__qualname__}.{field} needs a secret provider")
				elif not (isinstance(value, dict) and "key" in value):
					raise ValueError(f"{unit.__qualname__}.{field}" + " should be configured with a dict of the form {\"key\": \"secret-name\"}")
				target_type = getattr(secret_field, '__args__', [str])[0]
				init_config[field] = secrets.get(value["key"], target_type)

			# other model, possibly containing Secret fields
			elif type(typ) is ModelMetaclass:
				value = {}
				if field in init_config:
					value = init_config[field]
				elif field in defaults:
					value = defaults[field]
				init_config[field] = cls.resolve_secrets(secrets, typ, typ.__annotations__, typ.__field_defaults__, value)

			# Union, possibly containing other models
			elif get_origin(typ) is Union:
				value = {}
				if field in init_config:
					value = init_config[field]
				elif field in defaults:
					value = defaults[field]
				# skip if optional
				if value is None and type(None) in get_args(typ):
					continue
				for subtyp in get_args(typ):
					if type(subtyp) is ModelMetaclass:
						# configure for first model that satisfied by the initial value
						if isinstance(value, subtyp):
							break
						elif isinstance(value, dict) and set(value.keys()).union(subtyp.__field_defaults__) == set(subtyp.__fields__.keys()):
							init_config[field] = cls.resolve_secrets(secrets, subtyp, subtyp.__annotations__, subtyp.__field_defaults__, value)
							break
		return init_config


	def get_init_config(self,
		unit: Union[str, Type[AmpelBaseModel]],
		config: Optional[Union[int, str, Dict[str, Any]]] = None,
		override: Optional[Dict[str, Any]] = None,
		kwargs: Optional[Dict[str, Any]] = None,
		resolve_secrets = True,
	) -> Dict[str, Any]:
		""" :raises: ValueError is model type is not recognized """

		ret: Optional[Dict[str, Any]] = {}

		if isinstance(config, (dict, str)):
			ret = self.resolve_aliases(config)

		elif isinstance(config, int):
			if isinstance(
				confid := self.ampel_config.get(f"confid.{config}", dict, raise_exc=True),
				ReadOnlyDict
			):
				ret = recursive_unfreeze(confid)
			else:
				ret = confid

		if ret is None and config is not None:
			raise ValueError(f"Config alias {config} not found")

		ret = merge_dicts([ret, override, kwargs])
		if resolve_secrets and ret is not None:
			if isinstance(unit, str):
				unit = self.get_class_by_name(unit)
			if isinstance(unit, ModelMetaclass):
				annotations, defaults = unit.__annotations__, unit.__field_defaults__
			elif issubclass(unit, AmpelBaseModel):
				annotations, defaults = unit._annots, unit._defaults
			else:
				raise TypeError
			ret = self.resolve_secrets(self.secrets, unit, annotations, defaults, ret)
		return ret if ret is not None else {}


	@contextmanager
	def validate_unit_models(self) -> Iterator[None]:
		"""
		Enable validation for UnitModel instances
		"""
		extra_validator = (False, partial(_validate_unit_model, unit_loader=self))
		UnitModel.__post_root_validators__.append(extra_validator)
		AliasableModel._config = self.ampel_config
		try:
			yield
		finally:
			UnitModel.__post_root_validators__.remove(extra_validator)
			AliasableModel._config = None



	def get_resources(self, unit_model: UnitModel) -> Dict[str, Any]:
		"""
		Resources are defined using the static variable 'require' in ampel units
		-> example: catsHTM.default
		"""

		resources: Dict[str, Any] = {}

		if isinstance(unit_model.unit, str):
			unit_model.unit = self.get_class_by_name(unit_model.unit)

		# Load possibly required global resources
		for k in ampel_iter(getattr(unit_model.unit, 'require', [])):

			if k is None:
				continue

			# some unit require access to the channels definition
			if k == 'channel':
				resources[k] = self.ampel_config.get('channel')
				continue

			# Global resource example: extcat
			if (resource := self.ampel_config.get(f'resource.{k}')) is None:
				raise ValueError(f"Global resource not available: {k}")

			resources[k] = resource

		return resources


	@overload
	def get_class_by_name(self, name: str, unit_type: Type[T]) -> Type[T]:
		...
	@overload
	def get_class_by_name(self, name: str, unit_type: None = ...) -> Type[AmpelBaseModel]:
		...
	def get_class_by_name(self, name: str, unit_type: Optional[Type[T]] = None) -> Union[Type[T], Type[AmpelBaseModel]]:
		"""
		Matches the parameter 'name' with the unit definitions defined in the ampel_config.
		This allows to retrieve the corresponding fully qualified name of the class and to load it.

		:param unit_type:
			- DataUnit or any sublcass of DataUnit
			- AdminUnit or any sublcass of AdminUnit
			- If None, FQN will be retrieved from the auxiliary class conf entries and returned object will have Type[Any]

		:raises: ValueError if unit cannot be found or loaded or if parent class is unrecognized
		"""

		if name in AuxUnitRegister._defs:
			return AuxUnitRegister.get_aux_class(name, sub_type=unit_type)

		# Loop through list of class definition dicts
		fqn = None
		for udefs in self.unit_defs:
			if name in udefs:
				fqn = udefs[name]['fqn']
				break

		if fqn is None:
			raise ValueError(f"Ampel unit not found: {name}")

		# Note: importlib.import_module caches internally imported modules
		return getattr(import_module(fqn), name)


	@overload
	def new(self, unit_model: UnitModel, *, unit_type: Type[T], **kwargs) -> T:
		...
	@overload
	def new(self, unit_model: UnitModel, *, unit_type: None = ..., **kwargs) -> AmpelBaseModel:
		...
	def new(self, unit_model: UnitModel, *, unit_type: Optional[Type[T]] = None, **kwargs) -> Union[T, AmpelBaseModel]:
		"""
		Instantiate new object based on provided model and kwargs.
		:param 'unit_type': performs isinstance check and raise error on mismatch. Enables mypy/other static checks.
		"""

		if not isinstance(unit_model, UnitModel):
			raise ValueError(f"Unexpected model: '{type(unit_model)}'")

		if isinstance(unit_model.unit, str):
			unit_model.unit = self.get_class_by_name(unit_model.unit, unit_type)

		if unit_type:
			check_class(unit_model.unit, unit_type)

		init_args = self.get_init_config(
			unit_model.unit,
			unit_model.config,
			unit_model.override,
			kwargs
		)

		return unit_model.unit(**init_args) # type: ignore[call-arg]


	@overload
	def new_base_unit(self, unit_model: UnitModel, logger: AmpelLogger, *, sub_type: Type[BT], **kwargs) -> BT:
		...
	@overload
	def new_base_unit(self, unit_model: UnitModel, logger: AmpelLogger, *, sub_type: None = ..., **kwargs) -> DataUnit:
		...
	def new_base_unit(self,
		unit_model: UnitModel, logger: AmpelLogger, *, sub_type: Optional[Type[BT]] = None, **kwargs
	) -> Union[BT, DataUnit]:
		"""
		Base units require logger and resource as init parameters, additionaly to the potentialy
		defined custom parameters which will be provided as a union of the model config
		and the kwargs provided to this method (the latter having prevalance)
		:raises: ValueError is the unit defined in the model is unknown
		"""

		if sub_type is None or not issubclass(get_origin(sub_type) or sub_type, DataUnit):
			sub_type = cast(Type[BT], DataUnit) # remove cast when mypy gets smarter

		return self.new(
			unit_model, unit_type=sub_type, logger=logger, resource=self.get_resources(unit_model),
			**kwargs
		)


	@overload
	def new_admin_unit(self, unit_model: UnitModel, context: AmpelContext, *, sub_type: Type[PT], **kwargs) -> PT:
		...
	@overload
	def new_admin_unit(self, unit_model: UnitModel, context: AmpelContext, *, sub_type: None = ..., **kwargs) -> AdminUnit:
		...
	def new_admin_unit(self,
		unit_model: UnitModel, context: AmpelContext, *, sub_type: Optional[Type[PT]] = None, **kwargs
	) -> Union[AdminUnit, PT]:
		"""
		Processor units require a context as init parameters, additionaly to the potentialy
		defined custom parameters which will be provided as a union of the model config
		and the kwargs provided to this method (the latter having prevalance)
		:raises: ValueError is the unit defined in the model is unknown
		"""

		if sub_type is None or not issubclass(get_origin(sub_type) or sub_type, AdminUnit):
			sub_type = cast(Type[PT], AdminUnit) # remove cast when mypy gets smarter

		return self.new(
			unit_model, unit_type=sub_type, context=context,
			**kwargs
		)

	"""
	def internal_mypy_tests_uncomment_only_in_your_editor(self,
		model: UnitModel, context: AmpelContext, logger: AmpelLogger, sub_type: Optional[Type[PT]] = None, **kwargs
	) -> None:

		# Interal: uncomment to check if mypy works adequately

		from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
		from ampel.abstract.AbsLightCurveT2Unit import AbsLightCurveT2Unit

		reveal_type(self.new(model))
		reveal_type(self.new(model, bla=12))
		reveal_type(self.new(model, unit_type = None))
		reveal_type(self.new(model, unit_type=AbsLightCurveT2Unit))
		reveal_type(self.new(model, unit_type=AbsLightCurveT2Unit, bla=12))
		reveal_type(self.new(model, unit_type=AbsProcessorUnit))
		reveal_type(self.new(model, unit_type=AbsProcessorUnit, bla=12))

		reveal_type(self.new_base_unit(model, logger))
		reveal_type(self.new_base_unit(model, logger, bla=12))
		reveal_type(self.new_base_unit(model, logger, sub_type = None))
		reveal_type(self.new_base_unit(model, logger, sub_type=AbsLightCurveT2Unit))
		reveal_type(self.new_base_unit(model, logger, sub_type = AbsLightCurveT2Unit, bla=12))

		# Next two lines *should* fail
		reveal_type(self.new_base_unit(model, logger, sub_type=AbsProcessorUnit))
		reveal_type(self.new_base_unit(model, logger, sub_type = AbsProcessorUnit, bla=12))

		reveal_type(self.new_admin_unit(model, context))
		reveal_type(self.new_admin_unit(model, context, bla=12))
		reveal_type(self.new_admin_unit(model, context, sub_type = None))
		reveal_type(self.new_admin_unit(model, context, sub_type = AbsProcessorUnit))
		reveal_type(self.new_admin_unit(model, context, sub_type = AbsProcessorUnit, bla=12))

		# Next two lines *should* fail
		reveal_type(self.new_admin_unit(model, context, sub_type = AbsLightCurveT2Unit))
		reveal_type(self.new_admin_unit(model, context, sub_type = AbsLightCurveT2Unit, bla=12))
	"""


def _validate_unit_model(cls, values: Dict[str, Any], unit_loader: UnitLoader) -> Dict[str, Any]:
	"""
	Verify that a unit configuration is valid in the context of a specific UnitLoader.
	"""
	from ampel.base.DataUnit import DataUnit
	from ampel.core.AdminUnit import AdminUnit
	from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
	from ampel.abstract.ingest.AbsIngester import AbsIngester
	from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
	from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender

	unit = unit_loader.get_class_by_name(values['unit'])
	if issubclass(unit, (DataUnit, AdminUnit, AbsProcessorUnit, AbsIngester)):
		# exclude base class fields provided at runtime
		exclude = {"logger"}
		for parent in cast(
			Sequence[Type[AmpelBaseModel]],
			(
				DataUnit,
				AdminUnit,
				AbsT3UnitRunner,
				AbsT3RunContextAppender,
				AbsProcessorUnit,
				AbsIngester,
			)
		):
			if issubclass(unit, parent):
				exclude.update(set(parent._annots.keys()).difference(parent._defaults.keys()))
		fields = {
			k: (v, unit._defaults[k] if k in unit._defaults else ...)
			for k, v in unit._annots.items() if k not in exclude
		} # type: ignore
		model = create_model(
			unit.__name__, __config__ = StrictModel.__config__,
			__base__=None, __module__=None, __validators__=None,
			**fields
		)
		model.validate(
			unit_loader.get_init_config(
				values['unit'],
				values['config'],
				values['override']
			)
		)
	return values
