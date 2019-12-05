#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/AmpelLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib
from pydantic import BaseModel
from typing import Dict, Type, Union
from ampel.model.UnitData import UnitData
from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.AmpelConfig import AmpelConfig
from ampel.abstract.AbsAmpelUnit import AbsAmpelUnit
from ampel.abstract.AbsAmpelProcessor import AbsAmpelProcessor


class AmpelLoader:
	"""
	"""

	classes = {}


	def __init__(self, ampel_config: AmpelConfig, tier: int = None):
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


	def get_class(self, class_name: str, section="unit") -> Type:
		""" """

		# Load/Get unit *class* corresponding to provided unit id
		if class_name in AmpelLoader.classes:
			return AmpelLoader.classes[class_name]

		for tier in self.tiers:
			unit_def = self.ampel_config.get(
				f"{tier}.{section}.{class_name}"
			)
			if unit_def:
				break

		if unit_def is None:
			raise ValueError(
				f"Ampel {section} not found: {class_name}"
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
		AmpelLoader.classes[class_name] = UnitClass
		return UnitClass


	def get_init_config(
		self, unit_data: UnitData, klass: Type
	) -> Union[BaseModel, Dict[str, any]]:
		""" """		

		init_config = unit_data.init_config

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

		if unit_data.override:
			init_config = AmpelUtils.unflatten_dict(
				{
					**AmpelUtils.flatten_dict(init_config), 
					**AmpelUtils.flatten_dict(unit_data.override)
				}
			)

		# Check for possibly defined RunConfig model
		InnerInitConfigClass = getattr(klass, 'InitConfig', None)
		if InnerInitConfigClass:
			return InnerInitConfigClass(**init_config)

		return init_config
