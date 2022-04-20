#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/UnitConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  20.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys, re, importlib, traceback, pkg_resources
from os.path import join, basename, sep, relpath
from typing import Any

from ampel.protocol.LoggingHandlerProtocol import AggregatingLoggingHandlerProtocol
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.util.collections import ampel_iter
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.log import VERBOSE


class RemoteUnitDefinition(AmpelBaseModel):
	class_name: str
	base: list[str]


class UnitConfigCollector(ConfigCollector):


	def __init__(self, get_env: bool = True, **kwargs) -> None:
		self.get_env = get_env
		super().__init__(**kwargs)
		self.err_fqns: list[str] = []


	def add(self,
		arg: list[dict[str, str] | str],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:

		# Cosmetic
		if isinstance(self.logger.handlers[0], (AggregatingLoggingHandlerProtocol, AmpelStreamHandler)):
			agg_int = self.logger.handlers[0].aggregate_interval
			self.logger.handlers[0].aggregate_interval = 1000

		# tolerate list containing only 1 element defined as dict
		for el in ampel_iter(arg):

			try:

				if isinstance(el, str):

					# Package definition (ex: ampel.ztf.t3)
					# Auto-load units in defined package
					if el.split('.')[-1][0].islower():

						try:
							distrib = pkg_resources.get_distribution(dist_name)
							sources = distrib.get_resource_string(
								__name__, join(basename(distrib.egg_info), "SOURCES.txt")
							).decode('utf8')

							for line in sources.split('\n'):
								if sep in relpath(line, start=el.replace(".", sep)):
									continue
								self.add(
									line.replace(sep, ".").replace(".py", ""),
									dist_name, version, register_file
								)
						except Exception:
							self.logger.info(f'Units auto-registration has failed for package {el}')
						continue

					# Standart unit definition (ex: ampel.t3.stage.T3AggregatingStager)
					else:
						class_name = self.get_class_name(el)
						if not (mro := self.get_mro(el, class_name)):
							self.logger.break_aggregation()
							continue
						entry: dict[str, Any] = {
							'fqn': el,
							'base': mro,
							'distrib': dist_name,
							'file': register_file,
							'version': version
						}

				elif isinstance(el, dict):
					try:
						d = RemoteUnitDefinition(**el)
					except Exception:
						self.error(
							'Unsupported unit definition (dict)' +
							self.distrib_hint(dist_name, register_file)
						)
						continue

					class_name = d.class_name
					entry = {
						'base': d.base,
						'distrib': dist_name,
						'file': register_file,
						'version': version
					}

				else:
					self.error(
						'Unsupported unit config format' +
						self.distrib_hint(dist_name, register_file)
					)
					continue

				if self.get(class_name):
					self.duplicated_entry(
						conf_key = class_name,
						section_detail = f'{self.tier} {self.conf_section}',
						new_file = register_file,
						new_dist = dist_name,
						prev_file = self.get(class_name).get('conf', 'unknown') # type: ignore
					)
					continue

				if "AmpelUnit" not in entry['base'] and "AmpelBaseModel" not in entry['base']:
					self.logger.info(
						f'Unrecognized base class for {self.conf_section} {class_name} ' +
						self.distrib_hint(dist_name, register_file)
					)
					continue

				for base in ("AmpelABC", "AmpelUnit"):
					if base in entry["base"]:
						entry["base"].remove(base)

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
					self.distrib_hint(dist_name, register_file),
					exc_info=e
				)

		if isinstance(self.logger.handlers[0], AggregatingLoggingHandlerProtocol):
			self.logger.handlers[0].aggregate_interval = agg_int


	@staticmethod
	def get_class_name(fqn: str) -> str:
		"""
		Returns class name for a given fully qualified name
		Note: we here assume the ampel convention that module name equals class name
		(i.e that we have one class per module)
		"""
		return re.sub(r'.*\.', '', fqn) # noqa


	def get_mro(self, module_fqn: str, class_name: str) -> None | list[str]:
		"""
		:param module_fqn: fully qualified name of module
		:param class_name: declared class name in the module specified by "module_fqn"
		:returns: the method resolution order (except for the first and last members
		that is of no use for our purpose) of the specified class
		"""

		try:
			UnitClass = getattr(
				# import using fully qualified name
				importlib.import_module(module_fqn),
				class_name
			)
		except Exception:

			self.err_fqns.append(module_fqn)
			# Suppress superfluous traceback entries
			print("", file=sys.stderr)
			print("Cannot import " + module_fqn, file=sys.stderr)
			print("-" * (len(module_fqn) + 14), file=sys.stderr)
			for el in traceback.format_exc().splitlines():
				if "_bootstrap" not in el and "in import_module" not in el:
					print(el, file=sys.stderr)

			return None

		return [
			el.__name__
			for el in UnitClass.__mro__[:-1]
			if 'ampel' in el.__module__
		]
