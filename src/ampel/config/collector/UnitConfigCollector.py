#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/UnitConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 24.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import re, importlib
from typing import List, Union, Dict
from ampel.common.AmpelUtils import AmpelUtils
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.config.collector.TierConfigCollector import TierConfigCollector

class UnitDefinitionData(AmpelBaseModel):
	"""
	"""
	class_name: str
	short_mro: List[str]


class UnitConfigCollector(TierConfigCollector):
	""" """

	# pylint: disable=inconsistent-return-statements
	def add(self, arg: List[Union[Dict[str, str], str]], dist_name: str = None) -> None:
		"""
		"""
		for el in AmpelUtils.iter(arg):

			try:

				if isinstance(el, str):
					class_name = self.get_class_name(el)
					entry = {
						'fqn': el,
						'mro': self.get_trimmed_mro(el, class_name),
						'distName': dist_name
					}
					
				elif isinstance(el, Dict):
					try:
						d = UnitDefinitionData(**el)
					except Exception:
						self.error(
							"Unsupported unit definition (dict)" + 
							self.distrib_hint(dist_name)
						)
						continue

					class_name = d.class_name
					entry = {
						'mro': d.short_mro,
						'distName': dist_name
					}

				else:
					self.error(
						"Unsupported unit config format" + 
						self.distrib_hint(dist_name)
					)
					continue

				if self.get(class_name):
					self.duplicated_entry(
						conf_key=class_name, 
						section_detail=f"T{self.tier} {self.conf_section}",
						new_dist=dist_name
					)
					continue

				if self.verbose:
					self.logger.verbose(
						f"-> Adding T{self.tier} {self.conf_section}: {class_name}"
					)

				self.__setitem__(class_name, entry)

			except Exception as e:
				if 'class_name' not in locals():
					class_name = "Unknown"
				self.error(
					f"Error occured while loading {self.conf_section} {class_name} " +
					self.distrib_hint(dist_name),
					exc_info=e
				)


	@staticmethod
	def get_class_name(fqn: str) -> str:
		"""
		Returns class name for a given fully qualified name
		"""
		# pylint: disable=anomalous-backslash-in-string
		return re.sub(".*\.", "", fqn)


	@staticmethod
	def get_trimmed_mro(fqn: str, class_name: str) -> str:
		"""
		Returns method return order 
		(except for the first and last members 
		that is of no use for our purpose)
		for a given fully qualified name
		"""
		UnitClass = getattr(
			# import using fully qualified name
			importlib.import_module(fqn),
			class_name
		)

		for el in UnitClass.__mro__[1:-1]:
			print(el)
		return [
			UnitConfigCollector.get_class_name(el.__name__) 
			for el in UnitClass.__mro__[1:-1]
		]
