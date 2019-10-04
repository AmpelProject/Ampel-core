#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/AmpelUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.10.2019
# Last Modified Date: 04.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib
from typing import Dict, Union, ClassVar, Any
from ampel.db.DBUtils import DBUtils
from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.UnitConfig import UnitConfig
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.EncryptedConfig import EncryptedConfig
from ampel.base.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.base.abstract.AbsT3Unit import AbsT3Unit


class AmpelUnit:
	"""
	Central class for the modular ampel design
	"""

	classes = {}
	tier = {
		"AbsAlertFilter": 0,
		"AbsT0Unit": 0,
		"AbsT1Unit": 1,
		"AbsT2Unit": 2,
		"AbsT3Unit": 3,
	}

	def __init__(self, unit_config: UnitConfig, ampel_config: AmpelConfig):
		""" """
		self.unit_config = unit_config
		self.ampel_config = ampel_config


	def get_class(self):
		""" """

		# Load/Get unit *class* corresponding to provided unit id
		if self.unit_config.unit_id in AmpelUnit.classes:
			return AmpelUnit.classes[self.unit_config.unit_id]

		unit_def = self.ampel_config.get(
			"unit." + self.unit_config.unit_id
		)

		if unit_def is None:
			raise ValueError("Unknown ampel unit: "+ self.unit_config.unit_id)

		# get class object
		UnitClass = getattr(
			# import using fully qualified name
			importlib.import_module(unit_def['fqn']),
			self.unit_config.unit_id
		)

		# mro output example:
		# In []: c.__class__.__mro__
		# Out[]: (
		#	ampel.contrib.hu.t0.NoFilter.NoFilter,
		#	ampel.base.abstract.AbsAlertFilter.AbsAlertFilter,
		#	object
		# )
		abs_cls_name = UnitClass.__mro__[-2].__name__

		if abs_cls_name not in self.tier:
			raise ValueError(
				"Unknown parent class: %s. AmpelUnit.py probably needs an update" % 
				abs_cls_name
			)

		# Stamp class with adequate tier
		UnitClass.tier = self.tier[abs_cls_name]

		AmpelUnit.classes[self.unit_config.unit_id] = UnitClass
		return UnitClass


	def get_run_config(self):
		""" """
		run_config = self.unit_config.run_config

		if not run_config:
			return None

		# Load run config from alias if str was provided
		if type(run_config) is str:
			run_config = self.ampel_config.get(
				"alias." + run_config
			)

		elif type(run_config) is int:
			run_config = self.ampel_config.get(
				"runConfig.t2." + run_config
			)

		# Check for possibly defined RunConfig model
		InnerRunConfigClass = getattr(
			self.get_class(), 'RunConfig', None
		)

		if InnerRunConfigClass:
			return InnerRunConfigClass(**run_config)

		return run_config


	def get_run_config_id(self):
		""" """
		return DBUtils.b2_dict_hash(
			self.get_run_config()
		)


	def get_resources(self, context_repo: str):
		"""
		Global resources are defined in the static class variable 'resources' of the corresponding unit
		Global resource example: catsHTM

		Local resources are defined with the unit config key 'resources'.
		Local resource example: slack
		"""

		resources = {}

		# Load possibly required global resources 
		for k in AmpelUtils.iter(getattr(self.get_class(), 'resource', [])):

			resource = self.ampel_config.get('resource.'+k)
			if resource is None:
				raise ValueError("Global resource not available: "+ k)

			resources[k] = resource

		# Load possibly defined local resources 
		if self.unit_config.resources:

			local_resource = self.ampel_config.get('resource.' + context_repo)
			if local_resource is None:
				raise ValueError("No local resource defined for repo: "+ context_repo)

			for k in self.unit_config.resources:
				resources[k] = self.recursive_check_decrypt(
					local_resource[k], self.ampel_config
				)

		return resources


	@classmethod
	def recursive_check_decrypt(cls, arg: Dict, ampel_config: AmpelConfig) -> Dict:
		""" 
		"""
		ret = None

		for key in arg.keys():

			value = arg[key]

			if isinstance(value, Dict):

				if "iv" in value:
		
					if not ret:
						ret = arg.copy()

					try:
						ec = EncryptedConfig(**value)
						ret[key] = ec.decrypt(
							ampel_config.get("pwd")
						)
					except:
						cls.recursive_check_decrypt(value, ampel_config)
						continue

		return ret if ret else arg
