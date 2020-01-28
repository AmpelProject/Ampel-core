#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/DistConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 16.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources
from typing import Dict, Any, Callable, List
from ampel.config.builder.ConfigBuilder import ConfigBuilder


class DistConfigBuilder(ConfigBuilder):
	"""
	This child class of ConfigLoader allows to automcatically detect and load 
	various configuration files contained in ampel sub-packages installed with pip 
	See the methods 
	load_distributions(...) or
	load_distrib(...) or 
	load_ampel_conf_from_repo(...)
	"""

	def load_distributions(self, prefix: str = "ampel-", conf_dir: str = "conf") -> None:
		"""
		"""
		for distrib_name in self.get_dist_names(prefix):
			if self.verbose:
				self.logger.verbose("#"*150)
				self.logger.verbose("Checking " + distrib_name)
			self.load_distrib(distrib_name, conf_dir)


	def load_distrib(self, dist_name: str, conf_dir: str = "conf") -> None:
		"""
		"""
		try:

			pkg_dist = pkg_resources.get_distribution(dist_name)

			# Try root dir
			self.load_ampel_conf_from_repo(pkg_dist)

			# Try 'conf' dir (defined by parameter conf_dir)
			self.load_ampel_conf_from_repo(pkg_dist, conf_dir)

			# Look for dedicated "section configs" in provided conf dir
			if conf_dir in pkg_dist.resource_listdir(""):

				# Channel, db (and template) can be defined by multiple files
				# in a directory named after the corresponding config section name
				for key in ("channel", "db"):
					self.load_files_in_dir(
						pkg_dist, conf_dir, key, self.base_config[key].add
					)

				# ("channel", "db", "resource")
				for key in self.base_config.general_keys.keys():
					self.load_section_config_file(
						pkg_dist, conf_dir, f"{key}.conf", self.base_config[key].add
					)

				# ("controller", "processor", "unit", "alias", "process")
				for section in self.base_config.tier_keys.keys():
					self.load_sub_section_config_file(pkg_dist, conf_dir, section)

				# Try to load templates from folder template (defined by 'Ampel-ZTF' for ex.)
				self.load_files_in_dir(
					pkg_dist, conf_dir, "template", self.register_channel_templates
				)

				# Try to load templates from template.conf
				self.load_section_config_file(
					pkg_dist, conf_dir, "template.conf", self.register_channel_templates
				)

		except Exception as e:
			self.error = True
			self.logger.error(
				"Error occured while loading conf from distribution %s",
				dist_name, exc_info=e
			)


	def load_ampel_conf_from_repo(
		self, pkg_dist: pkg_resources.EggInfoDistribution, sub_dir: str = ""
	) -> None:
		""" """

		global_conf_path = f"{sub_dir}/ampel.conf" if sub_dir else "ampel.conf"

		try:

			# Global config
			if "ampel.conf" in pkg_dist.resource_listdir(sub_dir):

				if self.verbose:
					self.logger.verbose(
						"Loading %s from distribution %s",
						global_conf_path, pkg_dist.project_name
					)

				self.load_ampel_conf(
					json.loads(
						pkg_dist.get_resource_string(
							__name__, global_conf_path
						)
					),
					pkg_dist.project_name
				)

		except FileNotFoundError:
			pass

		except Exception as e:
			self.error = True
			self.logger.error(
				"Error while loading %s from distribution %s",
				global_conf_path, pkg_dist.project_name, exc_info=e
			)


	def load_section_config_file(
		self, pkg_dist: pkg_resources.EggInfoDistribution,
		conf_dir: str, conf_file: str, func: Callable[[Dict[str, Any]], None]
	) -> None:
		""" """

		try:

			if conf_file in pkg_dist.resource_listdir(conf_dir):

				if self.verbose:
					self.logger.verbose(
						"Loading %s/%s from distribution %s", conf_dir,
						conf_file, pkg_dist.project_name
					)

				func(
					json.loads(
						pkg_dist.get_resource_string(
							__name__, conf_dir + "/" + conf_file
						)
					),
					pkg_dist.project_name
				)

		except Exception as e:
			self.error = True
			self.logger.error(
				"Error occured while loading %s/%s from distribution %s",
				conf_dir, conf_file, pkg_dist.project_name, exc_info=e
			)



	def load_sub_section_config_file(
		self, pkg_dist: pkg_resources.EggInfoDistribution, conf_dir: str, section: str
	) -> None:
		""" 
		Files (<section>.conf) loaded by this method 
		must have the following JSON structure:
		{
			"t0": { ... },
			"t1": { ... },
			...
		}
		whereby the t0, t1, t2 and t3 keys are optional (at least one is required though)	
		"""

		try:

			conf_file = section + ".conf"

			if conf_file in pkg_dist.resource_listdir(conf_dir):

				if self.verbose:
					self.logger.verbose(
						"Loading %s/%s from distribution %s", conf_dir,
						conf_file, pkg_dist.project_name
					)

				d = json.loads(
					pkg_dist.get_resource_string(
						__name__, conf_dir + "/" + conf_file
					)
				)

				for k in ("t0", "t1", "t2", "t3"):
					if k in d:
						self.base_config[k][section].add(
							d[k], pkg_dist.project_name
						)

		except Exception as e:
			self.error = True
			self.logger.error(
				"Error occured while loading %s/%s from distribution %s",
				conf_dir, conf_file, pkg_dist.project_name, exc_info=e
			)


	def load_files_in_dir(
		self, pkg_dist: pkg_resources.EggInfoDistribution, conf_dir: str, 
		sub_dir_name: str, func: Callable[[Dict[str, Any]], None]
	) -> None:
		""" 
		Loads all files ending with .conf in a given directory
		and parse the dicts usings the provided Callable.
		"""
		if sub_dir_name in pkg_dist.resource_listdir(conf_dir):

			for file_name in pkg_dist.resource_listdir(conf_dir+"/"+sub_dir_name):

				if not file_name.endswith(".conf"):
					continue

				try:

					if self.verbose:
						self.logger.verbose(
							"Loading %s/%s/%s from distribution %s" % 
							(conf_dir, sub_dir_name, file_name, pkg_dist.project_name)
						)

					func(
						json.loads(
							pkg_dist.get_resource_string(
								__name__, "%s/%s/%s" % 
								(conf_dir, sub_dir_name, file_name)
							)
						),
						pkg_dist.project_name
					)

				except Exception as e:
					self.error = True
					self.logger.error(
						"Error occured while loading %s/%s/%s from distribution %s", conf_dir, 
						sub_dir_name, file_name, pkg_dist.project_name, exc_info=e
					)


	@staticmethod
	def get_dist_names(distrib_prefix: str="ampel-") -> List[str]:
		"""
		"""
		return [
			dist_name for dist_name in pkg_resources.AvailableDistributions() 
			if distrib_prefix in dist_name
		]	
