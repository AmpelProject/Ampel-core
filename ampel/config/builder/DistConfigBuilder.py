#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/builder/DistConfigBuilder.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.10.2019
# Last Modified Date:  14.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import json, yaml, pkg_resources, os, re
from functools import partial
from pkg_resources import EggInfoDistribution, DistInfoDistribution # type: ignore[attr-defined]
from typing import Any
from collections.abc import Callable
from ampel.config.builder.ConfigBuilder import ConfigBuilder
from ampel.util.distrib import get_dist_names, get_files
from ampel.log import VERBOSE, SHOUT


class DistConfigBuilder(ConfigBuilder):
	"""
	Subclass of ConfigLoader allowing to automatically detect and load
	various configuration files contained in ampel sub-packages installed with pip
	"""

	def load_distributions(self,
		prefixes: list[str] = ["pyampel-", "ampel-"],
		conf_dirs: list[str] = ["conf"],
		exts: list[str] = ["json", "yaml", "yml"]
	) -> None:
		"""
		:param prefixes: loads all known conf files from all distributions with name starting with prefixes
		:param conf_dirs: loads only conf files from these directories
		:param exts: loads only conf files with these extensions
		"""
		dist_names = get_dist_names(prefixes)

		if dist_names:
			self.logger.log(SHOUT, f"Detected ampel components: '{dist_names}'")

		for dist_name in dist_names:

			self.logger.break_aggregation()
			s = f"Checking distribution '{dist_name}'"
			self.logger.log(VERBOSE, s)
			self.logger.log(VERBOSE, "="*len(s))

			for conf_dir in conf_dirs:
				for ext in exts:
					self.load_distrib(dist_name, conf_dir, ext)

			if self.verbose:
				self.logger.log(VERBOSE, f"Done checking distribution '{dist_name}'")

		if self.verbose:
			self.logger.log(VERBOSE, "Done loading distributions")


	def load_distrib(self, dist_name: str, conf_dir: str = "conf", ext: str = "json") -> None:
		"""
		Loads all known conf files of the provided distribution (name)
		"""
		try:

			distrib = pkg_resources.get_distribution(dist_name)
			all_conf_files = get_files(dist_name, conf_dir, re.compile(f".*\.{ext}$")) # noqa

			if all_conf_files and self.verbose:
				self.logger.log(VERBOSE, "Following conf files will be parsed:")
				for el in all_conf_files:
					self.logger.log(VERBOSE, el)

			if ampel_conf := self.get_conf_file(all_conf_files, f"ampel.{ext}"):
				self.load_conf_using_func(distrib, ampel_conf, self.load_ampel_conf) # type: ignore

			# Channel, mongo (and template) can be defined by multiple files
			# in a directory named after the corresponding config section name
			for key in ("channel", "mongo", "process"):
				if specialized_conf_files := self.get_conf_files(all_conf_files, f"/{key}/"):
					for f in specialized_conf_files:
						self.load_conf_using_func(distrib, f, self.first_pass_config[key].add)

			# ("channel", "mongo", "resource")
			for key in self.first_pass_config.conf_keys.keys():
				if key == "alias":
					continue
				if section_conf_file := self.get_conf_file(all_conf_files, f"{key}.{ext}"):
					self.load_conf_using_func(distrib, section_conf_file, self.first_pass_config[key].add) # type: ignore

			# ("controller", "processor", "unit", "alias", "process")
			for unit_type in ("alias", "process"):
				if tier_conf_file := self.get_conf_file(all_conf_files, f"{unit_type}.{ext}"):
					self.load_conf_using_func(distrib, tier_conf_file, partial(self.register_tier_conf, unit_type))

			# Try to load templates from folder template (defined by 'Ampel-ZTF' for ex.)
			if template_conf_files := self.get_conf_files(all_conf_files, "/template/"):
				for f in template_conf_files:
					self.load_conf_using_func(distrib, f, self.register_channel_templates)

			# Try to load templates from template.conf
			if template_conf := self.get_conf_file(all_conf_files, "/template.{ext}"):
				self.load_conf_using_func(distrib, template_conf, self.register_channel_templates) # type: ignore

			if all_conf_files:
				self.logger.info(f"Not all conf files were loaded from distribution '{distrib.project_name}'")
				self.logger.info(f"Unprocessed conf files: {all_conf_files}")

		except Exception as e:
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
		distrib: EggInfoDistribution | DistInfoDistribution,
		file_rel_path: str,
		func: Callable[[dict[str, Any], str, str, str], None]
	) -> None:

		try:

			if self.verbose:
				self.logger.log(VERBOSE,
					f"Loading {file_rel_path} from distribution '{distrib.project_name}'"
				)

			if file_rel_path.endswith("json"):
				load = json.loads
			elif file_rel_path.endswith("yml") or file_rel_path.endswith("yaml"):
				load = yaml.safe_load # type: ignore
			
			if os.path.isabs(file_rel_path):
				with open(file_rel_path) as f:
					payload = f.read()
			else:
				payload = distrib.get_resource_string(__name__, file_rel_path)

			func(
				load(payload),
				distrib.project_name,
				distrib.version,
				file_rel_path
			)

		except FileNotFoundError:
			self.error = True
			self.logger.error(
				f"File '{file_rel_path}' not found in distribution '{distrib.project_name}'"
			)

		except Exception as e:
			self.error = True
			self.logger.error(
				f"Error occured loading '{file_rel_path}' from distribution '{distrib.project_name}'",
				exc_info=e
			)


	def load_tier_config_file(self,
		distrib: EggInfoDistribution | DistInfoDistribution,
		file_rel_path: str,
		root_key: str
	) -> None:
		"""
		Files loaded by this method must have the following JSON structure:
		{
			"t0": { ... },
			"t1": { ... },
			...
		}
		whereby the t0, t1, t2 and t3 keys are optional (at least one is required though)
		"""

		try:

			if self.verbose:
				self.logger.log(VERBOSE,
					f"Loading {file_rel_path} from distribution '{distrib.project_name}'"
				)

			if file_rel_path.endswith("json"):
				load = json.loads
			elif file_rel_path.endswith("yml") or file_rel_path.endswith("yaml"):
				load = yaml.safe_load # type: ignore

			d = load(
				distrib.get_resource_string(__name__, file_rel_path)
				if not os.path.isabs(file_rel_path)
				else file_rel_path
			)

			for k in ("t0", "t1", "t2", "t3", "ops"):
				if k in d:
					self.first_pass_config[root_key][k].add(
						d[k],
						dist_name = distrib.project_name,
						version = distrib.version,
						register_file = file_rel_path,
					)

		except FileNotFoundError:
			self.error = True
			self.logger.error(
				f"File '{file_rel_path}' not found in distribution '{distrib.project_name}'"
			)

		except Exception as e:
			self.error = True
			self.logger.error(
				f"Error occured loading '{file_rel_path}' "
				f"from distribution '{distrib.project_name}')",
				exc_info=e
			)


	@staticmethod
	def get_conf_file(files: list[str], key: str) -> None | str:
		"""
		Extract the first entry who matches the provided 'key' from the provided list
		Note: this method purposely modifies the input list (removes matched element)
		"""
		for el in files:
			if key in el:
				files.remove(el)
				return el
		return None


	@staticmethod
	def get_conf_files(files: list[str], key: str) -> list[str]:
		"""
		Extract all entries who matches the provided 'key' from the provided list
		Note: this method purposely modifies the input list (removes matched elements)
		"""
		ret = [el for el in files if key in el]
		for el in ret:
			files.remove(el)
		return ret
