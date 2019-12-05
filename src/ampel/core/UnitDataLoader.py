#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/AmpelUnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Type, Any, Union
from ampel.common.AmpelUtils import AmpelUtils
from ampel.core.AmpelUnit import AmpelUnit
from ampel.core.AmpelLoader import AmpelLoader
from ampel.abstract.AbsT2Unit import AbsT2Unit
from ampel.model.UnitData import UnitData
from ampel.model.AmpelBaseModel import AmpelBaseModel


class AmpelUnitLoader(AmpelLoader):
	"""
	Central class of the modular ampel design, 
	whose purpose it to hold information related to 
	user contributed unit (Filters, T2s, T3s) and configuration
	"""


	def get_ampel_unit(self, unit_data: UnitData) -> AmpelUnit:
		""" """

		klass = self.get_class(unit_data.class_name)

		return AmpelUnit(
			unit_class = klass,
			init_config = self.get_init_config(unit_data, klass),
			resources = self.get_resources(unit_data, klass)
		)


	def get_run_config(
		self, unit_data: UnitData, klass: Type
	) -> Union[AmpelBaseModel, Dict[str, any]]:
		""" """

		if not issubclass(klass, AbsT2Unit):
			raise ValueError("Method only available for T2 units")

		run_config = unit_data.run_config

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

		if unit_data.override:
			run_config = AmpelUtils.unflatten_dict(
				{
					**AmpelUtils.flatten_dict(run_config), 
					**AmpelUtils.flatten_dict(unit_data.override)
				}
			)

		# Check for possibly defined RunConfig model
		InnerRunConfigClass = getattr(klass, 'RunConfig', None)
		if InnerRunConfigClass:
			return InnerRunConfigClass(**run_config)

		return run_config


	def get_resources(self, unit_data: UnitData, klass: Type) -> Dict[str, Any]:
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
		if unit_data.resources:

			for k in unit_data.resources:

				local_resource = self.ampel_config.get('resource.' + k)
				if not local_resource:
					raise ValueError(
						f"Local resource '{k}' not found "
					)

				resources[k] = self.ampel_config.recursive_decrypt(local_resource)

		return resources
