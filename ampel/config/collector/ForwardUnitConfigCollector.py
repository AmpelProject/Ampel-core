#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ForwardUnitConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.03.2020
# Last Modified Date: 02.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import re, importlib
from typing import List, Union, Dict, Optional, Any, Sequence
from ampel.model.builder.UnitDefinitionModel import UnitDefinitionModel
from ampel.config.collector.UnitConfigCollector import UnitConfigCollector
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.AbsForwardConfigCollector import AbsForwardConfigCollector

tier_class: Dict[str, List[str]] = {
	"t0": ["AbsPhotoAlertFilter", "AbsT0Unit", "AlertProcessor"],
	"t1": [""],
	"t2": ["AbsT2DataPointUnit", "AbsT2StateUnit", "AbsT2StockUnit"],
	"t3": ["AbsT3Unit"]
}

path_el_type: Dict[str, str] = {
	"base": "AbsDataUnit",
	"aux": "AbsAuxiliaryUnit",
	"processor": "AbsProcessorUnit",
	"controller": "AbsControllerUnit",
}


class ForwardUnitConfigCollector(AbsForwardConfigCollector):
	""" """

	def get_path(self,
		arg: Union[Dict[str, Any], str],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> Optional[Sequence[Union[str, int]]]:

		try:

			if isinstance(arg, str):
				class_id = arg
				mro = self.get_trimmed_mro(arg, UnitConfigCollector.get_class_name(arg))

			elif isinstance(arg, dict):
				try:
					d = UnitDefinitionModel(**arg)
				except Exception:
					self.error(
						"Unsupported unit definition (dict)" +
						ConfigCollector.distrib_hint(file_name, dist_name)
					)
					return None

				mro = d.short_mro
				class_id = d.class_name

			else:
				self.error(
					"Unsupported unit config format" +
					ConfigCollector.distrib_hint(file_name, dist_name)
				)
				return None

			tier = None
			unit_type = None

			for k, v in tier_class.items():
				if [klass for klass in mro if klass in v]:
					tier = k

			if tier is None:
				self.error(f"Could not auto-associate a tier with unit {class_id}")
				return None

			for k in path_el_type:
				if path_el_type[k] in mro:
					unit_type = k

			if unit_type is None:
				self.error(f"Could not auto-associate a type with unit {class_id}")
				return None

			# check for AbsDataUnit in mro ?
			if self.debug:
				self.logger.verbose(
					f"-> Routing {self.conf_section} unit '{class_id}' to {tier}.unit.{unit_type}"
				)

			return [tier, "unit", unit_type]

		except Exception as e:

			if 'class_id' not in locals():
				class_id = "Unknown"

			self.error(
				f"Error occured while loading {self.conf_section} {class_id} " +
				ConfigCollector.distrib_hint(file_name, dist_name),
				exc_info=e
			)

			return None


	@staticmethod
	def get_class_name(fqn: str) -> str:
		"""
		Returns class name for a given fully qualified name
		Note: we here assume the ampel convention that module name equals class name
		(i.e that we have one class per module)
		"""
		# pylint: disable=anomalous-backslash-in-string
		return re.sub(".*\.", "", fqn) # noqa


	@staticmethod
	def get_trimmed_mro(mfqn: str, class_name: str) -> List[str]:
		"""
		:param mfqn: fully qualified name of module
		:param class_name: declared class name in the module specified by mfqn
		:returns: the method resolution order (except for the first and last members
		that is of no use for our purpose) of the specified class
		"""
		UnitClass = getattr(
			# import using fully qualified name
			importlib.import_module(mfqn),
			class_name
		)

		return [
			UnitConfigCollector.get_class_name(el.__name__)
			for el in UnitClass.__mro__[1:-1]
		]
