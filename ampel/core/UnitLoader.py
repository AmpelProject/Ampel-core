#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/UnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 01.08.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from typing import ( # type: ignore[attr-defined]
	Dict, Type, Any, Union, Optional, ClassVar, TypeVar,
	Literal, List, _GenericAlias, overload, get_origin, cast
)

from ampel.util.collections import ampel_iter
from ampel.util.mappings import flatten_dict, unflatten_dict
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.base.DataUnit import DataUnit
from ampel.core.AmpelContext import AmpelContext
from ampel.model.UnitModel import UnitModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.AdminUnit import AdminUnit

T = TypeVar('T', bound=AmpelBaseModel)
BT = TypeVar('BT', bound=DataUnit)
PT = TypeVar('PT', bound=AdminUnit)


# flake8: noqa: E704
class UnitLoader:

	# References unit definitions of other auxiliary units
	# to allow aux units to be able to load other aux units
	aux_defs: ClassVar[Dict[str, Any]] = {}

	def __init__(self, config: AmpelConfig, tier: Optional[Literal[0, 1, 2, 3]] = None) -> None:
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
			config._config["unit"]["aux"]
		]

		self.aliases: List[Dict] = [
			config._config['alias'][f"t{el}"] for el in (0, 3, 1, 2)
		]



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
			return {k: self.resolve_aliases(v) for k,v in value.items()}
		else:
			return value


	def get_init_config(self,
		config: Optional[Union[int, str, Dict[str, Any]]] = None,
		override: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		""" :raises: ValueError is model type is not recognized """

		if not config:
			return {}

		ret: Optional[Dict[str, Any]] = None

		if isinstance(config, (dict, str)):
			ret = self.resolve_aliases(config)

		elif isinstance(config, int):
			ret = self.ampel_config.get(f"confid.{config}", dict)

		if not ret:
			raise ValueError(f"Config alias {config} not found")

		if ret and override:
			return unflatten_dict(
				{**flatten_dict(ret), **flatten_dict(override)} # type: ignore
			)

		return ret


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
	def get_class_by_name(self, name: str, unit_type: Type[T]) -> Type[T]: ...
	@overload
	def get_class_by_name(self, name: str, unit_type: None = ...) -> Type[AmpelBaseModel]: ...

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

		if name in UnitLoader.aux_defs:
			return UnitLoader.get_aux_class(name, sub_type=unit_type)

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
	def new(self, unit_model: UnitModel, *, unit_type: Type[T], **kwargs) -> T: ...
	@overload
	def new(self, unit_model: UnitModel, *, unit_type: None = ..., **kwargs) -> AmpelBaseModel: ...

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
			self.check_class(unit_model.unit, unit_type)

		return unit_model.unit(
			**{**self.get_init_config(unit_model.config, unit_model.override), **kwargs} # py3.9 will allow more concise writing
		) # type: ignore[call-arg]


	@overload
	def new_base_unit(self, unit_model: UnitModel, logger: AmpelLogger, *, sub_type: Type[BT], **kwargs) -> BT: ...
	@overload
	def new_base_unit(self, unit_model: UnitModel, logger: AmpelLogger, *, sub_type: None = ..., **kwargs) -> DataUnit: ...

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
			**{**self.get_init_config(unit_model.config, unit_model.override), **kwargs}
		)


	@overload
	def new_admin_unit(self, unit_model: UnitModel, context: AmpelContext, *, sub_type: Type[PT], **kwargs) -> PT: ...
	@overload
	def new_admin_unit(self, unit_model: UnitModel, context: AmpelContext, *, sub_type: None = ..., **kwargs) -> AdminUnit: ...

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
			**{**self.get_init_config(unit_model.config, unit_model.override), **kwargs}
		)


	@overload
	@staticmethod
	def new_aux_unit(unit_model: UnitModel, *, sub_type: Type[T], **kwargs) -> T: ...
	@overload
	@staticmethod
	def new_aux_unit(unit_model: UnitModel, *, sub_type: None = ..., **kwargs) -> AmpelBaseModel: ...

	@staticmethod
	def new_aux_unit(
		unit_model: UnitModel, *, sub_type: Optional[Type[T]] = None, **kwargs
	) -> Union[T, AmpelBaseModel]:
		"""	:raises: ValueError is unit_model.config is not of type Optional[dict] """

		Klass = UnitLoader.get_aux_class(klass=unit_model.unit, sub_type=sub_type)
		if unit_model.config:
			if isinstance(unit_model.config, dict):
				return Klass(**{**unit_model.config, **kwargs}) # type: ignore[call-arg]
			raise ValueError("Auxiliary units cannot use config aliases")
		return Klass(**kwargs) # type: ignore[call-arg]


	@overload
	@staticmethod
	def get_aux_class(klass: Union[str, Type], *, sub_type: Type[T]) -> Type[T]: ...
	@overload
	@staticmethod
	def get_aux_class(klass: Union[str, Type], *, sub_type: None = ...) -> Type[AmpelBaseModel]: ...

	@staticmethod
	def get_aux_class(klass: Union[str, Type], *, sub_type: Optional[Type[T]] = None) -> Union[Type[T], Type[AmpelBaseModel]]:
		""" :raises: ValueError if unit is unknown """

		if isinstance(klass, str):
			if klass not in UnitLoader.aux_defs:
				raise ValueError(f"Unknown auxiliary unit {klass}")

			fqn = UnitLoader.aux_defs[klass]['fqn']
			# we 'redefine' klass and mypy doesn't like that, hence the ignores below
			klass = getattr(import_module(fqn), fqn.split('.')[-1])

		if sub_type:
			UnitLoader.check_class(klass, sub_type) # type: ignore[arg-type]

		return klass # type: ignore[return-value]


	@staticmethod
	def check_class(Klass: Type, class_type: Union[Type[AmpelBaseModel], _GenericAlias]) -> None:
		""" :raises: ValueError """

		if isinstance(class_type, _GenericAlias):
			class_type = get_origin(class_type)
		if not issubclass(Klass, class_type):
			raise ValueError(f"{Klass} is not a subclass of {class_type}")


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
