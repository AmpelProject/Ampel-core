#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/UnitConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 03.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import re, importlib
from typing import List, Union, Dict, Optional, Any
from ampel.util.collections import ampel_iter
from ampel.util.crypto import b2_short_hash
from ampel.model.StrictModel import StrictModel
from ampel.config.collector.AbsListConfigCollector import AbsListConfigCollector
from ampel.log import VERBOSE


class RemoteUnitDefinition(StrictModel):
	class_name: str
	base: List[str]


class UnitConfigCollector(AbsListConfigCollector):

	def add(self,
		arg: List[Union[Dict[str, str], str]],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:

		# tolerate list containing only 1 element defined as dict
		for el in ampel_iter(arg):

			try:

				if isinstance(el, str):
					class_name = self.get_class_name(el)
					entry: Dict[str, Any] = {
						'fqn': el,
						'base': self.get_mro(el, class_name),
						'distrib': dist_name,
						'file': file_name
					}

				elif isinstance(el, dict):
					try:
						d = RemoteUnitDefinition(**el)
					except Exception:
						self.error(
							'Unsupported unit definition (dict)' +
							self.distrib_hint(file_name, dist_name)
						)
						continue

					class_name = d.class_name
					entry = {
						'base': d.base,
						'distrib': dist_name,
						'file': file_name
					}

				else:
					self.error(
						'Unsupported unit config format' +
						self.distrib_hint(file_name, dist_name)
					)
					continue

				if self.get(class_name):
					self.duplicated_entry(
						conf_key = class_name,
						section_detail = f'{self.tier} {self.conf_section}',
						new_file = file_name,
						new_dist = dist_name,
						prev_file = self.get(class_name).get('conf', 'unknown') # type: ignore
					)
					continue

				if "AmpelBaseModel" not in entry['base']:
					self.logger.info(
						f'Unrecognized base class for {self.conf_section} {class_name} ' +
						self.distrib_hint(file_name, dist_name)
					)
					return

				if self.conf_section == "admin unit":
					if "AdminUnit" not in entry['base']:
						raise ValueError(f"AdminUnit missing for admin unit {entry}")
					entry['base'].remove("AdminUnit")

				elif self.conf_section == "base unit":

					# Hash class name of T2 units
					for el in entry['base']:
						if 'T2Unit' in el:
							entry['hash'] = b2_short_hash(class_name)
							break

					if "DataUnit" not in entry['base']:
						raise ValueError(f"DataUnit missing for base unit {entry}")
					entry['base'].remove("DataUnit")

				entry['base'] = entry['base'][:-2]
				if self.verbose:
					self.logger.log(VERBOSE,
						f'Adding {self.conf_section}: {class_name}'
					)

				self.__setitem__(class_name, entry)

			except Exception as e:

				if 'class_name' not in locals():
					class_name = 'Unknown'

				self.error(
					f'Error occured while loading {self.conf_section} {class_name} ' +
					self.distrib_hint(file_name, dist_name),
					exc_info=e
				)


	@staticmethod
	def get_class_name(fqn: str) -> str:
		"""
		Returns class name for a given fully qualified name
		Note: we here assume the ampel convention that module name equals class name
		(i.e that we have one class per module)
		"""
		# pylint: disable=anomalous-backslash-in-string
		return re.sub(r'.*\.', '', fqn) # noqa


	@staticmethod
	def get_mro(module_fqn: str, class_name: str) -> List[str]:
		"""
		:param module_fqn: fully qualified name of module
		:param class_name: declared class name in the module specified by "module_fqn"
		:returns: the method resolution order (except for the first and last members
		that is of no use for our purpose) of the specified class
		"""
		UnitClass = getattr(
			# import using fully qualified name
			importlib.import_module(module_fqn),
			class_name
		)

		return [
			UnitConfigCollector.get_class_name(el.__name__)
			for el in UnitClass.__mro__[1:-1]
			if 'ampel' in el.__module__
		]
