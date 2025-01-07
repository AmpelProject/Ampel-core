#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/builder/DistConfigBuilder.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.10.2019
# Last Modified Date:  18.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import re
from collections.abc import Callable, Sequence
from functools import partial
from importlib import metadata
from typing import Any

import yaml

from ampel.config.builder.ConfigBuilder import ConfigBuilder
from ampel.log import SHOUT, VERBOSE
from ampel.util.distrib import PathLike, PathList, get_dist_names, get_files


class DistConfigBuilder(ConfigBuilder):
	"""
	Subclass of ConfigLoader allowing to automatically detect and load
	various configuration files contained in ampel sub-packages installed with pip
	"""

	def load_distributions(self,
		prefixes: Sequence[str] = ("pyampel-", "ampel-"),
		conf_dirs: Sequence[str] = ("conf",),
		exts: Sequence[str] = ("json", "yaml", "yml"),
		raise_exc: bool = True,
		exclude: None | list[str] = None
	) -> None:
		"""
		:param prefixes: loads all known conf files from all distributions with name starting with prefixes
		:param conf_dirs: loads only conf files from these directories
		:param exts: loads only conf files with these extensions
		:param exclude: exclude distribution by name
		"""
		dist_names = get_dist_names(prefixes)

		if dist_names:
			self.logger.log(SHOUT, f"Detected ampel components: '{dist_names}'")

		if exclude:
			for i in range(len(exclude)):
				exclude[i] = exclude[i].lower()

		for dist_name in dist_names:

			self.logger.break_aggregation()

			if exclude and dist_name.lower() in exclude:
				self.logger.log(VERBOSE, "Excluding distribution '{dist_name}' as requested")
				continue

			s = f"Checking distribution '{dist_name}'"
			self.logger.log(VERBOSE, s)
			self.logger.log(VERBOSE, "="*len(s))

			for conf_dir in conf_dirs:
				for ext in exts:
					self.load_distrib(dist_name, conf_dir, ext, raise_exc=raise_exc)

			if self.verbose:
				self.logger.log(VERBOSE, f"Done checking distribution '{dist_name}'")

		if self.verbose:
			self.logger.log(VERBOSE, "Done loading distributions")


	def load_distrib(self,
		dist_name: str,
		conf_dir: str = "conf",
		ext: str = "json",
		raise_exc: bool = True
	) -> None:
		"""
		Loads all known conf files of the provided distribution (name)
		"""
		try:

			distrib = metadata.distribution(dist_name)
			all_conf_files = get_files(dist_name, conf_dir, re.compile(rf".*\.{ext}$"))

			if all_conf_files and self.verbose:
				self.logger.log(VERBOSE, "Following conf files will be parsed:")
				for el in all_conf_files:
					self.logger.log(VERBOSE, el.as_posix())

			if ampel_conf := self.get_conf_file(all_conf_files, f"ampel.{ext}"):
				self.load_conf_using_func(
					distrib, ampel_conf, self.load_ampel_conf, raise_exc=raise_exc
				)

			# Channel, mongo (and template) can be defined by multiple files
			# in a directory named after the corresponding config section name
			for key in ("channel", "mongo", "process"):
				if specialized_conf_files := self.get_conf_files(all_conf_files, f"*/{key}/*"):
					for f in specialized_conf_files:
						self.load_conf_using_func(distrib, f, self.first_pass_config[key].add)

			# ("channel", "mongo", "resource")
			for key in self.first_pass_config.conf_keys:
				if key == "alias":
					continue
				if section_conf_file := self.get_conf_file(all_conf_files, f"{key}.{ext}"):
					self.load_conf_using_func(distrib, section_conf_file, self.first_pass_config[key].add)

			# ("controller", "processor", "unit", "alias", "process")
			for unit_type in ("alias", "process"):
				if tier_conf_file := self.get_conf_file(all_conf_files, f"{unit_type}.{ext}"):
					self.load_conf_using_func(
						distrib, tier_conf_file, partial(self.register_tier_conf, unit_type)
					)

			# Try to load templates from folder template (defined by 'Ampel-ZTF' for ex.)
			if template_conf_files := self.get_conf_files(all_conf_files, "*/template/*"):
				for f in template_conf_files:
					self.load_conf_using_func(distrib, f, self.register_templates)

			# Try to load templates from template.conf
			if template_conf := self.get_conf_file(all_conf_files, "template.{ext}"):
				self.load_conf_using_func(distrib, template_conf, self.register_templates)

			if all_conf_files:
				self.logger.info(f"Not all conf files were loaded from distribution '{distrib.name}'")
				self.logger.info(f"Unprocessed conf files: {all_conf_files}")

		except Exception as e:
			if raise_exc:
				raise
			self.error = True
			self.logger.error(
				f"Error occured while loading configuration files from the distribution '{dist_name}'",
				exc_info=e
			)


	def register_tier_conf(self,
		root_key: str,
		d: dict[str, Any],
		dist_name: str,
		version: str,
		file_rel_path: str,
	):
		for k in ("t0", "t1", "t2", "t3", "ops"):
			if k in d:
				self.first_pass_config[root_key][k].add(
					d[k],
					dist_name = dist_name,
					version = version,
					register_file = file_rel_path,
				)


	def load_conf_using_func(self,
		distrib: metadata.Distribution,
		file_rel_path: "PathLike",
		func: Callable[[dict[str, Any], str, str, str], None],
		raise_exc: bool = True,
		**kwargs
	) -> None:

		try:

			if self.verbose:
				self.logger.log(VERBOSE,
					f"Loading {file_rel_path} from distribution '{distrib.name}'"
				)

			# NB: read both YAML and JSON (which is a subset of YAML)
			content = yaml.safe_load(file_rel_path.read_text())

			func(
				content,
				distrib.name,
				distrib.version,
				file_rel_path.as_posix(),
				**kwargs
			)

		except FileNotFoundError:
			if raise_exc:
				raise
			self.error = True
			self.logger.error(
				f"File '{file_rel_path}' not found in distribution '{distrib.name}'"
			)

		except Exception as e:
			if raise_exc:
				raise
			self.error = True
			self.logger.error(
				f"Error occured loading '{file_rel_path}' from distribution '{distrib.name}'",
				exc_info=e
			)


	@staticmethod
	def get_conf_file(files: "PathList", key: str) -> "None | PathLike":
		"""
		Extract the first entry who matches the provided 'key' from the provided list
		Note: this method purposely modifies the input list (removes matched element)
		"""
		for el in files:
			if el.match(key):
				files.remove(el)
				return el
		return None


	@staticmethod
	def get_conf_files(files: "PathList", key: str) -> "PathList":
		"""
		Extract all entries who matches the provided 'key' from the provided list
		Note: this method purposely modifies the input list (removes matched elements)
		"""
		ret = [el for el in files if el.match(key)]
		for el in ret:
			files.remove(el)
		return ret
