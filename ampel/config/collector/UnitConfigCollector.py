#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/UnitConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  02.01.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import importlib
import os
import re
import sys
import traceback
from contextlib import contextmanager
from os.path import sep
from typing import Any

from xxhash import xxh64_intdigest

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.protocol.LoggingHandlerProtocol import AggregatingLoggingHandlerProtocol
from ampel.util.collections import ampel_iter
from ampel.util.distrib import get_files


class RemoteUnitDefinition(AmpelBaseModel):
	class_name: str
	base: list[str]


class UnitConfigCollector(AbsDictConfigCollector):


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.err_fqns: list[tuple[str, Exception]] = []


	def add(self, # type: ignore[override]
		arg: list[dict[str, str] | str],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:

		# Cosmetic
		if isinstance(self.logger.handlers[0], AggregatingLoggingHandlerProtocol | AmpelStreamHandler):
			agg_int = self.logger.handlers[0].aggregate_interval
			self.logger.handlers[0].aggregate_interval = 1000

		# tolerate list containing only 1 element defined as dict
		for el in ampel_iter(arg):

			try:

				if isinstance(el, str):

					# Package definition (ex: ampel.ztf.t3)
					# Auto-load units in defined package
					if el.split('.')[-1][0].islower():
						package_path = el.replace(".", sep)
						try:
							for fpath in map(str, get_files(dist_name, lookup_dir=package_path)):
								if sep in fpath.replace(package_path + sep, ""):
									continue
								self.add(
									[fpath.replace(sep, ".").replace(".py", "")],
									dist_name, version, register_file
								)
						except Exception:
							self.logger.info(f'Units auto-registration has failed for package {el}')
							self.has_error = True
						continue

					# Standart unit definition (ex: ampel.t3.stage.T3AggregatingStager)
					class_name = self.get_class_name(el)
					if not (ret := self.get_mro(el, class_name)):
						self.logger.break_aggregation()
						continue
					entry: dict[str, Any] = {
						'fqn': el,
						'base': ret[1],
						'xxh64': ret[0]
					}

				elif isinstance(el, dict):
					try:
						d = RemoteUnitDefinition.validate(el)
					except Exception:
						self.error(
							'Unsupported unit definition (dict)' +
							self.distrib_hint(dist_name, register_file)
						)
						continue

					class_name = d.class_name
					entry = {'base': d.base}

				else:
					self.error(
						'Unsupported unit config format' +
						self.distrib_hint(dist_name, register_file)
					)
					continue

				if self.check_duplicates(class_name, dist_name, version, register_file):
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

				if self.options.verbose:
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

				self.has_error = True

		if isinstance(self.logger.handlers[0], AggregatingLoggingHandlerProtocol):
			self.logger.handlers[0].aggregate_interval = agg_int


	@staticmethod
	def get_class_name(fqn: str) -> str:
		"""
		Returns class name for a given fully qualified name
		Note: we here assume the ampel convention that module name equals class name
		(i.e that we have one class per module)
		"""
		return re.sub(r'.*\.', '', fqn)


	def get_mro(self, module_fqn: str, class_name: str) -> None | tuple[int, list[str]]:
		"""
		:param module_fqn: fully qualified name of module
		:param class_name: declared class name in the module specified by "module_fqn"
		:returns: the method resolution order (except for the first and last members
		that is of no use for our purpose) of the specified class
		"""

		try:
			# contextlib.redirect_stderr does not work with C-loaded backends (ex: annoying healpix warnings)
			with stderr_redirected(self.options.hide_stderr):
				# import using fully qualified name
				m = importlib.import_module(module_fqn)
				# note: can't be a built-in module for which __file__ does not exist
				with open(m.__file__, "rb") as f: # type: ignore[arg-type]
					digest = xxh64_intdigest(f.read())
				UnitClass = getattr(m, class_name)
		except Exception as e:

			self.err_fqns.append((module_fqn, e))
			self.has_error = True

			if self.options.hide_module_not_found_errors and isinstance(e, ModuleNotFoundError):
				return None

			# Suppress superfluous traceback entries
			print("", file=sys.stderr)
			print("Cannot import " + module_fqn, file=sys.stderr)
			print("-" * (len(module_fqn) + 14), file=sys.stderr)
			for el in traceback.format_exc().splitlines():
				if "_bootstrap" not in el and "in import_module" not in el:
					print(el, file=sys.stderr)

			return None

		return digest, [
			el.__name__
			for el in UnitClass.__mro__[:-1]
			if 'ampel' in el.__module__
		]


# contextlib.redirect_stderr does not work with C-loaded backends (ex: annoying healpix warnings)
# Code below is borrowed from:
# https://stackoverflow.com/questions/4675728/redirect-stdout-to-a-file-in-python/22434262#22434262
def fileno(file_or_fd):
	fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
	if not isinstance(fd, int):
		raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
	return fd


@contextmanager
def stderr_redirected(activated: bool = False):

	if not activated:
		yield
		return

	stderr = sys.stderr
	stderr_fd = fileno(stderr)
	# copy stderr_fd before it is overwritten
	# NOTE: `copied` is inheritable on Windows when duplicating a standard stream
	with os.fdopen(os.dup(stderr_fd), 'wb') as copied:
		stderr.flush()  # flush library buffers that dup2 knows nothing about
		try:
			os.dup2(fileno(os.devnull), stderr_fd)  # $ exec >&os.devnull
		except ValueError:  # filename
			with open(os.devnull, 'wb') as to_file:
				os.dup2(to_file.fileno(), stderr_fd)  # $ exec > os.devnull
		try:
			yield stderr # allow code to be run with the redirected stderr
		finally:
			# restore stderr to its previous value
			# NOTE: dup2 makes stderr_fd inheritable unconditionally
			stderr.flush()
			os.dup2(copied.fileno(), stderr_fd)  # $ exec >&copied
