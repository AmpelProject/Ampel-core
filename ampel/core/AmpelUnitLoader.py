#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/AmpelUnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 06.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib
from pydantic import BaseModel
from typing import Dict, Type, Any, Union, Optional

from ampel.model.UnitModel import UnitModel
from ampel.common.AmpelUtils import AmpelUtils
from ampel.abstract.AbsT2Unit import AbsT2Unit
from ampel.config.AmpelConfig import AmpelConfig
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.abstract.AbsAmpelUnit import AbsAmpelUnit
from ampel.abstract.AbsAmpelProcessor import AbsAmpelProcessor


class AmpelUnitLoader:
	"""
	"""

	classes = {}


	def __init__(self, ampel_config: AmpelConfig, tier: Optional[int] = None):
		""" 
		For optimization purposes, please try to set the parameter tier.
		For example, a T2 controller should spawn an AmpelUnitLoader 
		using AmpelUnitLoader(ampel_config, 2).
		"""
		self.ampel_config = ampel_config
		if tier is not None:
			self.tiers = [f"t{tier}"] + [f"t{el}" for el in (0, 1, 2, 3) if el != tier]
		else:
			self.tiers = [f"t{el}" for el in (0, 1, 2, 3) if el != tier]


	def get_unit_class(self, unit_model: UnitModel) -> Type:
		""" """
		return self.get_class(unit_model.class_name)


	def get_unit_instance(
		self, unit_model: UnitModel, logger: AmpelLogger
	) -> AbsAmpelUnit:
		""" """
		klass = self.get_unit_class(unit_model)
		return klass(
			logger,
			self.get_init_config(unit_model, klass),
			self.get_resources(unit_model, klass),
		)


	def get_processor_class(self, unit_model: UnitModel) -> Type:
		""" """
		return self.get_class(unit_model.class_name, category="processor")


	def get_processor_instance(
		self, unit_model: UnitModel, 
		options: Optional[Union[BaseModel, Dict[str, Any]]] = None
	) -> AbsAmpelProcessor:
		""" """
		klass = self.get_unit_class(unit_model)
		return klass(
			self.ampel_config, 
			self.get_init_config(unit_model, klass),
			options
		)
		

	def get_class(self, class_name: str, category="unit") -> Type:
		""" """

		# Load/Get unit *class* corresponding to provided unit id
		if class_name in self.classes:
			return self.classes[class_name]

		for tier in self.tiers:
			unit_def = self.ampel_config.get(
				f"{tier}.{category}.{class_name}"
			)
			if unit_def:
				break

		if unit_def is None:
			raise ValueError(
				f"Ampel {category} not found: {class_name}"
			)

		# get class object
		UnitClass = getattr(
			# import using fully qualified name
			importlib.import_module(unit_def['fqn']),
			class_name
		)

		if not issubclass(UnitClass, (AbsAmpelProcessor, AbsAmpelUnit)):
			raise ValueError("Unrecognized parent class")

		# waiting for walrus operator
		self.classes[class_name] = UnitClass
		return UnitClass


	def get_init_config(
		self, unit_model: UnitModel, klass: Type
	) -> Union[BaseModel, Dict[str, any]]:
		""" """		

		init_config = unit_model.init_config

		if not init_config:
			return None

		# Load run config from alias
		if isinstance(init_config, str):

			alias = init_config
			for tier in self.tiers:
				init_config = self.ampel_config.get(
					f"t{tier}.alias.{init_config}"
				)
				if init_config:
					break

			if not init_config:
				raise ValueError(
					f"Alias {alias} not found"
				)

		if unit_model.override:
			init_config = AmpelUtils.unflatten_dict(
				{
					**AmpelUtils.flatten_dict(init_config), 
					**AmpelUtils.flatten_dict(unit_model.override)
				}
			)

		# Check for possibly defined InitConfig model
		InnerInitConfigClass = getattr(klass, 'InitConfig', None)
		if InnerInitConfigClass:
			return InnerInitConfigClass(**init_config)

		return init_config


	def get_run_config(
		self, unit_model: UnitModel, klass: Type
	) -> Union[BaseModel, Dict[str, any]]:
		""" """

		if not issubclass(klass, AbsT2Unit):
			raise ValueError("Method only available for T2 units")

		run_config = unit_model.run_config

		if not run_config:
			return None

		# Load run config from hashed run_config id (T2 only)
		run_config = self.ampel_config.get(
			f"t2.runConfig.{run_config}"
		)

		if not run_config:
			raise ValueError(
				"T2 run config {run_config} not found"
			)

		if unit_model.override:
			run_config = AmpelUtils.unflatten_dict(
				{
					**AmpelUtils.flatten_dict(run_config), 
					**AmpelUtils.flatten_dict(unit_model.override)
				}
			)

		# Check for possibly defined RunConfig model
		InnerRunConfigClass = getattr(klass, 'RunConfig', None)
		if InnerRunConfigClass:
			return InnerRunConfigClass(**run_config)

		return run_config


	def get_resources(self, unit_model: UnitModel, klass: Type) -> Dict[str, Any]:
		"""
		Global resources are defined in the static class variable 'resources' of the corresponding unit
		Global resource example: catsHTM.default

		Local resources are defined with the unit config key 'resources'.
		Local resource example: slack
		"""

		resources = {}

		# Load possibly required global resources 
		for k in AmpelUtils.iter(
			getattr(klass, 'resource', [])
		):

			resource = self.ampel_config.get('resource.'+k)
			if resource is None:
				raise ValueError("Global resource not available: "+ k)

			resources[k] = resource

		# Load possibly defined local resources 
		if unit_model.resources:

			for k in unit_model.resources:

				local_resource = self.ampel_config.get('resource.' + k)
				if not local_resource:
					raise ValueError(
						f"Local resource '{k}' not found "
					)

				resources[k] = self.ampel_config.recursive_decrypt(local_resource)

		return resources
