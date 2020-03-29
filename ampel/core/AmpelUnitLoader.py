#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelUnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib
from typing import ( # type: ignore[attr-defined]
	Dict, Type, Any, Union, Optional, ClassVar, TypeVar,
	Literal, _GenericAlias, overload, get_origin
)

from ampel.types import ProcUnitModels, DataUnitModels, AllUnitModels
from ampel.utils.collections import ampel_iter
from ampel.utils.mappings import flatten_dict, unflatten_dict

from ampel.abstract.AbsDataUnit import AbsDataUnit
from ampel.model.PlainUnitModel import PlainUnitModel
from ampel.model.DataUnitModel import DataUnitModel
from ampel.model.AliasedUnitModel import AliasedUnitModel
from ampel.model.AliasedDataUnitModel import AliasedDataUnitModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.abstract.AbsAuxiliaryUnit import AbsAuxiliaryUnit
from ampel.core.AmpelContext import AmpelContext

BT = TypeVar('BT', bound=AbsDataUnit)
AT = TypeVar('AT', bound=AbsAuxiliaryUnit)
PT = TypeVar('PT', bound=AbsProcessorUnit)


class AmpelUnitLoader:
	""" """

	# References unit definitions of other auxiliary units
	# to allow aux units to be able to load other aux units
	aux: ClassVar[Dict[str, Any]] = {}
	aux_classes: ClassVar[Dict[str, Type]] = {}

	def __init__(self,
		config: AmpelConfig,
		tier: Optional[Literal[0, 1, 2, 3]] = None
	) -> None:
		"""
		For optimization purposes, try to set the parameter tier.
		For example, a T2 controller should spawn an AmpelUnitLoader
		using AmpelUnitLoader(ampel_config, 2).

		:raises: ValueError in case bad arguments are provided
		"""

		if not isinstance(config, AmpelConfig):
			raise ValueError("First parameter must be an instance of AmpelConfig")

		self.ampel_config = config
		self.classes: Dict[str, Type] = {}

		# Register static aux units
		AmpelUnitLoader.aux = config.get("aux", dict) # type: ignore

		if tier is not None:
			self.tiers = [f"t{tier}"] + [f"t{el}" for el in (0, 1, 2, 3) if el != tier]
		else:
			self.tiers = [f"t{el}" for el in (0, 1, 2, 3) if el != tier]

		if tier:
			AmpelUnitLoader.aux.update(config.get(f"t{tier}.aux", dict)) # type: ignore


	@overload
	def get_class(self, class_name: str, unit_type: Literal['processor']) -> Type[AbsProcessorUnit]:
		...

	@overload
	def get_class(self, class_name: str, unit_type: Literal['aux']) -> Type[AbsAuxiliaryUnit]:
		...

	@overload
	def get_class(self, class_name: str, unit_type: Literal['base']) -> Type[AbsDataUnit]:
		...

	def get_class(self,
		class_name: str,
		unit_type: Literal['processor', 'aux', 'base']
	) -> Type[Union[AbsProcessorUnit, AbsAuxiliaryUnit, AbsDataUnit]]:
		"""
		Note: regarding unit_type, with 'unit' is meant data unit
		(processor units and auxiliary units are units too)
		:raises: ValueError if unit cannot be found or if parent class is unrecognized
		"""

		# Load/Get unit *class* corresponding to provided unit id
		if class_name in self.classes:
			return self.classes[class_name]

		# Check static aux_classes
		if unit_type == 'aux':
			return self.get_aux_class(
				class_name = class_name,
				sub_type = AbsAuxiliaryUnit
			)

		for tier in self.tiers:
			unit_def = self.ampel_config.get(
				f"{tier}.unit.{unit_type}.{class_name}", dict
			)
			if unit_def:
				break

		if unit_def is None:
			raise ValueError(
				f"Ampel {unit_type} unit not found: {class_name}"
			)

		# get class object
		UnitClass = getattr(
			# import using fully qualified name
			importlib.import_module(unit_def['fqn']),
			class_name
		)

		if not issubclass(UnitClass, (AbsProcessorUnit, AbsAuxiliaryUnit, AbsDataUnit)):
			raise ValueError("Unrecognized parent class")

		self.classes[class_name] = UnitClass
		return UnitClass


	def get_init_config(self, model: AllUnitModels) -> Optional[Dict[str, Any]]:
		"""
		:raises: ValueError is model type is not recognized
		"""

		if not model.config:
			return None

		if isinstance(model, (PlainUnitModel, DataUnitModel)):
			return model.config

		# Note: AliasedDataUnitModel is sub-class of AliasedUnitModel
		if not isinstance(model, (PlainUnitModel, DataUnitModel)):
			raise ValueError(f"Unrecognized config type {type(model)}")

		ret = None

		# Load init config from alias
		if isinstance(model.config, (int, str)):

			# AliasedUnitModel with an int config value should come from models
			# defined at T0 or T1 level (t2_compute) of T2 configs
			if isinstance(model.config, int):
				if ret := self.ampel_config.get(f"t2.config.{model.config}", dict):
					return ret

			for tier in self.tiers:
				ret = self.ampel_config.get(f"t{tier}.alias.{model.config}", dict)
				if ret:
					break

			if not ret:
				raise ValueError(
					f"Alias {model.config} not found"
				)

		elif isinstance(model.config, dict):
			ret = model.config

		if ret and isinstance(model, AliasedUnitModel) and model.override:
			ret = unflatten_dict(
				{
					**flatten_dict(ret),
					**flatten_dict(model.override)
				}
			)

		return ret


	def get_resources(self, model: DataUnitModel, Klass: Type) -> Dict[str, Any]:
		"""
		Global resources are defined in the static class variable 'resources' of the corresponding unit
		Global resource example: catsHTM.default

		Local resources are defined with the unit config key 'resources'.
		Local resource example: slack
		"""

		resources: Dict[str, Any] = {}

		# Load possibly required global resources
		for k in ampel_iter(getattr(Klass, 'require', [])):
			if k is None:
				continue
			if k == 'channel': # global channel options/policies
				resources[k] = self.ampel_config.get('channel')
				continue
			if resource := self.ampel_config.get(f'resource.{k}') is None:
				raise ValueError(f"Global resource not available: {k}")
			resources[k] = resource

		# Load possibly defined local resources
		if model.resource:

			for k in model.resource:

				local_resource = self.ampel_config.get(f'resource.{k}', dict)
				if not local_resource:
					raise ValueError(f"Local resource '{k}' not found ")

				resources[k] = self.ampel_config.recursive_decrypt(f'resource.{k}')

		return resources


	# Units
	#######

	@overload
	def new_base_unit(self, *,
		model: DataUnitModels, logger: AmpelLogger
	) -> AbsDataUnit:
		...

	@overload
	def new_base_unit(self, *,
		model: DataUnitModels, logger: AmpelLogger, sub_type: None, **kwargs
	) -> AbsDataUnit:
		...

	@overload
	def new_base_unit(self, *,
		model: DataUnitModels, logger: AmpelLogger, sub_type: Type[BT], **kwargs
	) -> BT:
		...

	def new_base_unit(self, *,
		model: DataUnitModels, logger: AmpelLogger, sub_type: Optional[Type[BT]] = None, **kwargs
	) -> Union[BT, AbsDataUnit]:
		""" """

		if not isinstance(model, (DataUnitModel, AliasedDataUnitModel)):
			raise ValueError(f"Unexpected model: '{type(model)}'")

		Klass = self.get_class(model.unit, unit_type="base")

		if isinstance(sub_type, _GenericAlias):
			sub_type = get_origin(sub_type)

		if not issubclass(Klass, sub_type if sub_type else AbsDataUnit):
			raise ValueError(f"Unexpected type: '{type(Klass)}'")

		conf = self.get_init_config(model)
		if conf is None:
			conf = {}

		return Klass( # type: ignore
			logger = logger,
			resource = self.get_resources(model, Klass),
			**conf,
			**kwargs
		)


	# Processors
	############

	@overload
	def get_processor_class(self, *, model: ProcUnitModels) -> Type[AbsProcessorUnit]:
		...

	@overload
	def get_processor_class(self, *, model: ProcUnitModels, sub_type: Type[PT]) -> Type[PT]:
		...

	def get_processor_class(self, *,
		model: ProcUnitModels, sub_type: Optional[Type[PT]] = None
	) -> Type[Union[AbsProcessorUnit, PT]]:
		""" Shortcut method """
		return self.get_class(model.unit, unit_type="processor")


	@overload
	def new_processor_unit(self, *,
		model: ProcUnitModels, context: AmpelContext, sub_type: None
	) -> AbsProcessorUnit:
		...

	@overload
	def new_processor_unit(self, *,
		model: ProcUnitModels, context: AmpelContext, sub_type: None, **kwargs
	) -> AbsProcessorUnit:
		...

	@overload
	def new_processor_unit(self, *,
		model: ProcUnitModels, context: AmpelContext, sub_type: Type[PT], **kwargs
	) -> PT:
		...

	def new_processor_unit(self, *,
		model: ProcUnitModels,
		context: AmpelContext,
		sub_type: Optional[Type[PT]] = None,
		**kwargs
	) -> Union[AbsProcessorUnit, PT]:
		"""
		:raises: ValueError
		"""
		Klass = self.get_class(model.unit, unit_type="processor")
		required_type = sub_type if sub_type else AbsProcessorUnit
		if not issubclass(Klass, required_type): # type: ignore
			raise ValueError(
				f"Unexpected type:\nRequired: {required_type}\n"
				f"Found: '{Klass.mro()}'"
			)

		if init_config := self.get_init_config(model):
			kwargs = {**init_config, **kwargs} # type: ignore

		return Klass(context=context, **kwargs)


	# Auxiliary units
	#################

	@classmethod
	@overload
	def get_aux_class(cls, *, class_name: str, sub_type: None) -> Type[AbsAuxiliaryUnit]:
		...

	@classmethod
	@overload
	def get_aux_class(cls, *, class_name: str, sub_type: Type[AT]) -> Type[AT]:
		...

	@classmethod
	def get_aux_class(cls, *,
		class_name: str, sub_type: Optional[Type[AT]] = None
	) -> Type[Union[AbsAuxiliaryUnit, AT]]:
		"""
		:raises: ValueError if unit is unknown
		"""

		if class_name not in cls.aux:
			raise ValueError(f"Unknown auxiliary unit {class_name}")

		if class_name in cls.aux_classes:
			return cls.aux_classes[class_name]
		else:

			# get class object
			UnitClass = getattr(
				# import using fully qualified name
				importlib.import_module(cls.aux[class_name]['fqn']),
				class_name
			)

			if sub_type:
				if not issubclass(UnitClass, sub_type):
					raise ValueError(f"Unrecognized parent class (must be a subclass of {sub_type})")
			else:
				if not issubclass(UnitClass, AbsAuxiliaryUnit):
					raise ValueError("Unrecognized parent class")

			cls.aux_classes[class_name] = UnitClass
			return UnitClass


	@classmethod
	@overload
	def new_aux_unit(cls, *,
		model: PlainUnitModel, sub_type: Type[AT]
	) -> AT:
		...

	@classmethod
	@overload
	def new_aux_unit(cls, *,
		model: PlainUnitModel, sub_type: None, **kwargs
	) -> AbsAuxiliaryUnit:
		...

	@classmethod
	@overload
	def new_aux_unit(cls, *,
		model: PlainUnitModel, sub_type: Type[AT], **kwargs
	) -> AT:
		...

	@classmethod
	def new_aux_unit(cls, *,
		model: PlainUnitModel,
		sub_type: Optional[Type[AT]] = None,
		**kwargs
	) -> Union[AT, AbsAuxiliaryUnit]:

		Klass = cls.get_aux_class(
			class_name = model.unit,
			sub_type = sub_type
		)

		if sub_type:
			if not issubclass(Klass, sub_type):
				raise ValueError(f"{model.unit} is not a subclass of {sub_type}")
		else:
			if not issubclass(Klass, AbsAuxiliaryUnit):
				raise ValueError("Unrecognized parent class")

		if model.config:
			return Klass(**{**model.config, **kwargs}) # type: ignore[call-arg]

		return Klass(**kwargs) # type: ignore[call-arg]
