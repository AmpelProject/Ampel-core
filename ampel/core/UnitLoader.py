#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/UnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 07.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from typing import ( # type: ignore[attr-defined]
	Dict, Type, Any, Union, Optional, ClassVar, TypeVar,
	Literal, List, _GenericAlias, overload, get_origin, cast
)

from ampel.type import T
from ampel.util.collections import ampel_iter
from ampel.util.mappings import flatten_dict, unflatten_dict
from ampel.core.AmpelContext import AmpelContext
from ampel.model.PlainUnitModel import PlainUnitModel
from ampel.model.DataUnitModel import DataUnitModel
from ampel.model.AliasedUnitModel import AliasedUnitModel
from ampel.model.AliasedDataUnitModel import AliasedDataUnitModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsDataUnit import AbsDataUnit
from ampel.abstract.AbsAdminUnit import AbsAdminUnit

BT = TypeVar('BT', bound=AbsDataUnit)
PT = TypeVar('PT', bound=AbsAdminUnit)

ProcUnitModels = Union[PlainUnitModel, AliasedUnitModel]
DataUnitModels = Union[DataUnitModel, AliasedDataUnitModel]
AllUnitModels = Union[PlainUnitModel, AliasedUnitModel, DataUnitModel, AliasedDataUnitModel]

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
			config._config['unit']['base'],
			config._config['unit']['admin'],
			config._config["unit"]["core"],
			config._config["unit"]["aux"]
		]

		self.aliases: List[Dict] = [
			config._config['alias'][f"t{el}"] for el in (0, 3, 1, 2)
		]

		# Register static aux units
		# UnitLoader.aux_defs = config.get("aux", dict) # type: ignore

		#if tier and config.get(f"t{tier}.aux", dict) is not None:
		#	UnitLoader.aux_defs.update(config.get(f"t{tier}.aux", dict)) # type: ignore


	def get_init_config(self, model: AllUnitModels) -> Dict[str, Any]:
		""" :raises: ValueError is model type is not recognized """

		if not model.config:
			return {}

		if isinstance(model, (PlainUnitModel, DataUnitModel)):
			return model.config

		# Note: AliasedDataUnitModel is sub-class of AliasedUnitModel
		if not isinstance(model, AliasedUnitModel):
			raise ValueError(f"Unrecognized config type {type(model)}")

		ret = {}

		# Load init config from alias
		if isinstance(model.config, (int, str)):

			# AliasedUnitModel with an int config value should come from models
			# defined at T0 or T1 level (t2_compute) of T2 configs
			if isinstance(model.config, int):
				if ret := self.ampel_config.get(f"t2.config.{model.config}", dict):
					return ret

			for adict in self.aliases:
				if model.config in adict:
					ret = adict[model.config]
					break

			if not ret:
				raise ValueError(f"Alias {model.config} not found")

		if ret and model.override:
			return unflatten_dict(
				{**flatten_dict(ret), **flatten_dict(model.override)}
			)

		return ret


	def get_resources(self, model: DataUnitModel, Klass: Optional[Type] = None) -> Dict[str, Any]:
		"""
		Global resources are defined in the static class variable 'resource' of the corresponding unit
		example: catsHTM.default
		Local resources are defined with the unit config key 'resource'.
		example: slack
		"""

		resources: Dict[str, Any] = {}

		if not Klass:
			Klass = self.get_class_by_name(model.unit)

		# Load possibly required global resources
		for k in ampel_iter(getattr(Klass, 'require', [])):

			if k is None:
				continue

			# some unit can require access to the channel definitions
			if k == 'channel':
				resources[k] = self.ampel_config.get('channel')
				continue

			# Global resource example: extcat
			if resource := self.ampel_config.get(f'resource.{k}') is None:
				raise ValueError(f"Global resource not available: {k}")
			resources[k] = resource

		# Load possibly defined local resources
		if model.resource:
			for k in model.resource:

				# Local resource example: a report unit might in some case /
				# for some channel report to slack and in other cases not
				if local_resource := self.ampel_config.get(f'resource.{k}', dict):
					resources[k] = local_resource
				else:
					raise ValueError(f"Local resource '{k}' not found ")

		return resources


	@overload
	def get_class_by_name(self, name: str, unit_type: Type[T]) -> Type[T]: ...
	@overload
	def get_class_by_name(self, name: str, unit_type: None = ...) -> Type: ...

	def get_class_by_name(self, name: str, unit_type: Optional[Type[T]] = None) -> Union[Type[T], Type]:
		"""
		Matches the parameter 'name' with the unit definitions defined in the ampel_config.
		This allows to retrieve the corresponding fully qualified name of the class and to load it.
		Note: if you have the fqn and class name at hand, rather use the static method load_class(...)

		:param unit_type:
		- AbsDataUnit or any sublcass of AbsDataUnit
		- AbsAdminUnit or any sublcass of AbsAdminUnit
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

		return UnitLoader.load_class(fqn, name, class_type=unit_type)


	@overload
	def new(self, model: AllUnitModels, *, unit_type: Type[T], **kwargs) -> T: ...
	@overload
	def new(self, model: AllUnitModels, *, unit_type: None = ..., **kwargs) -> object: ...

	def new(self, model: AllUnitModels, *, unit_type: Optional[Type[T]] = None, **kwargs) -> Union[T, object]:
		"""
		Instantiate new object based on provided model and kwargs.
		:param 'unit_type': performs isinstance check and raise error on mismatch. Enables mypy/other static checks.
		"""

		if not isinstance(model, PlainUnitModel):
			raise ValueError(f"Unexpected model: '{type(model)}'")

		return self.get_class_by_name(model.unit, unit_type)(
			**{**self.get_init_config(model), **kwargs} # py3.9 will allow more concise writing
		) # type: ignore[call-arg]


	@overload
	def new_base_unit(self, model: DataUnitModels, logger: AmpelLogger, *, sub_type: Type[BT], **kwargs) -> BT: ...
	@overload
	def new_base_unit(self, model: DataUnitModels, logger: AmpelLogger, *, sub_type: None = ..., **kwargs) -> AbsDataUnit: ...

	def new_base_unit(self,
		model: DataUnitModels, logger: AmpelLogger, *, sub_type: Optional[Type[BT]] = None, **kwargs
	) -> Union[BT, AbsDataUnit]:
		"""
		Base units require logger and resource as init parameters, additionaly to the potentialy
		defined custom parameters which will be provided as a union of the model config
		and the kwargs provided to this method (the latter having prevalance)
		:raises: ValueError is the unit defined in the model is unknown
		"""

		if sub_type is None or not issubclass(get_origin(sub_type) or sub_type, AbsDataUnit):
			sub_type = cast(Type[BT], AbsDataUnit) # remove cast when mypy gets smarter

		return self.new(
			model, unit_type=sub_type, logger=logger, resource=self.get_resources(model),
			**{**self.get_init_config(model), **kwargs}
		)


	@overload
	def new_admin_unit(self, model: ProcUnitModels, context: AmpelContext, *, sub_type: Type[PT], **kwargs) -> PT: ...
	@overload
	def new_admin_unit(self, model: ProcUnitModels, context: AmpelContext, *, sub_type: None = ..., **kwargs) -> AbsAdminUnit: ...

	def new_admin_unit(self,
		model: ProcUnitModels, context: AmpelContext, *, sub_type: Optional[Type[PT]] = None, **kwargs
	) -> Union[AbsAdminUnit, PT]:
		"""
		Processor units require a context as init parameters, additionaly to the potentialy
		defined custom parameters which will be provided as a union of the model config
		and the kwargs provided to this method (the latter having prevalance)
		:raises: ValueError is the unit defined in the model is unknown
		"""

		if sub_type is None or not issubclass(get_origin(sub_type) or sub_type, AbsAdminUnit):
			sub_type = cast(Type[PT], AbsAdminUnit) # remove cast when mypy gets smarter

		return self.new(
			model, unit_type=sub_type, context=context,
			**{**self.get_init_config(model), **kwargs}
		)


	@overload
	@staticmethod
	def new_aux_unit(model: PlainUnitModel, *, sub_type: Type[T], **kwargs) -> T: ...
	@overload
	@staticmethod
	def new_aux_unit(model: PlainUnitModel, *, sub_type: None = ..., **kwargs) -> object: ...

	@staticmethod
	def new_aux_unit(
		model: PlainUnitModel, *, sub_type: Optional[Type[T]] = None, **kwargs
	) -> Union[T, object]:

		Klass = UnitLoader.get_aux_class(class_name=model.unit, sub_type=sub_type)
		if model.config:
			return Klass(**{**model.config, **kwargs}) # type: ignore[call-arg]
		return Klass(**kwargs) # type: ignore[call-arg]


	@overload
	@staticmethod
	def get_aux_class(class_name: str, *, sub_type: Type[T]) -> Type[T]: ...
	@overload
	@staticmethod
	def get_aux_class(class_name: str, *, sub_type: None = ...) -> Type: ...

	@staticmethod
	def get_aux_class(class_name: str, *, sub_type: Optional[Type[T]] = None) -> Union[Type[T], Type]:
		""" :raises: ValueError if unit is unknown """

		if class_name not in UnitLoader.aux_defs:
			raise ValueError(f"Unknown auxiliary unit {class_name}")

		return UnitLoader.load_class(
			UnitLoader.aux_defs[class_name]['fqn'], class_name, class_type=sub_type
		)


	@overload
	@staticmethod
	def load_class(fqn: str, class_name: Optional[str] = ..., *, class_type: Type[T]) -> Type[T]: ...
	@overload
	@staticmethod
	def load_class(fqn: str, class_name: Optional[str] = ..., *, class_type: None = ...) -> Type: ...

	@staticmethod
	def load_class(fqn: str, class_name: Optional[str] = None, *, class_type: Optional[Type[T]] = None) -> Union[Type[T], Type]:
		"""
		Load class using fully qualified name example: ampel.log.AmpelLogger
		Note: importlib caches internally imported modules
		"""

		# get class object
		UnitClass = getattr(
			import_module(fqn),
			class_name if class_name else fqn.split('\n')[-1]
		)

		if class_type:
			if isinstance(class_type, _GenericAlias):
				class_type = get_origin(class_type)
			if not issubclass(UnitClass, class_type):
				raise ValueError(f"Unrecognized parent class: '{type(UnitClass)}'")

		return UnitClass


	"""
	def internal_mypy_tests_uncomment_only_in_your_editor(self,
		model1: ProcUnitModels, model2: DataUnitModels, model3: AllUnitModels,
		context: AmpelContext, logger: AmpelLogger, sub_type: Optional[Type[PT]] = None, **kwargs
	) -> None:

		# Interal: uncomment to check if mypy works adequately

		from ampel.abstract.AbsRunnable import AbsRunnable
		from ampel.abstract.AbsLightCurveT2Unit import AbsLightCurveT2Unit

		reveal_type(self.new(model3))
		reveal_type(self.new(model3, bla=12))
		reveal_type(self.new(model3, unit_type = None))
		reveal_type(self.new(model3, unit_type=AbsLightCurveT2Unit))
		reveal_type(self.new(model3, unit_type=AbsLightCurveT2Unit, bla=12))
		reveal_type(self.new(model3, unit_type=AbsRunnable))
		reveal_type(self.new(model3, unit_type=AbsRunnable, bla=12))

		reveal_type(self.new_base_unit(model2, logger))
		reveal_type(self.new_base_unit(model2, logger, bla=12))
		reveal_type(self.new_base_unit(model2, logger, sub_type = None))
		reveal_type(self.new_base_unit(model2, logger, sub_type=AbsLightCurveT2Unit))
		reveal_type(self.new_base_unit(model2, logger, sub_type = AbsLightCurveT2Unit, bla=12))
		reveal_type(self.new_base_unit(model2, logger, sub_type=AbsRunnable))
		reveal_type(self.new_base_unit(model2, logger, sub_type = AbsRunnable, bla=12))

		reveal_type(self.new_admin_unit(model1, context))
		reveal_type(self.new_admin_unit(model1, context, bla=12))
		reveal_type(self.new_admin_unit(model1, context, sub_type = None))
		reveal_type(self.new_admin_unit(model1, context, sub_type = AbsRunnable))
		reveal_type(self.new_admin_unit(model1, context, sub_type = AbsRunnable, bla=12))
		reveal_type(self.new_admin_unit(model1, context, sub_type = AbsLightCurveT2Unit))
		reveal_type(self.new_admin_unit(model1, context, sub_type = AbsLightCurveT2Unit, bla=12))
	"""
