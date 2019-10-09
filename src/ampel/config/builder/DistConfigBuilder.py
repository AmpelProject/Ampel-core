#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/DistConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 09.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources, re
from typing import Dict, Callable, List, Union

from ampel.db.DBUtils import DBUtils
from ampel.common.AmpelUtils import AmpelUtils
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.config.builder.ConfigBuilder import ConfigBuilder


class DistConfigBuilder(ConfigBuilder):
	"""
	This child class of ConfigLoader allows to automcatically detect and load 
	various configuration files contained in ampel sub-packages installed with pip 
	See the methods 
	load_distributions(...) or
	load_distrib(...) or 
	load_global_conf_from_repo(...)
	"""

	def load_distributions(self, prefix: str="ampel-", conf_dir: str="conf") -> None:
		"""
		"""
		for distrib_name in self.get_repo_names(prefix):
			if self.verbose:
				self.logger.verbose("Checking "+distrib_name)
			self.load_distrib(distrib_name)


	def load_distrib(self, repo_name: str, conf_dir: str="conf") -> None:
		"""
		"""
		try:

			pkg_dist = pkg_resources.get_distribution(repo_name)

			self.load_global_conf_from_repo(pkg_dist)
			self.load_global_conf_from_repo(pkg_dist, conf_dir)

			# general config
			if conf_dir in pkg_dist.resource_listdir(""):

				self.load_dir_files(
					pkg_dist, conf_dir, "channels", self.add_channels
				)

				self.load_dir_files(
					pkg_dist, conf_dir, "jobs", self.add_job
				)

				for key, val in self.func_load_file.items():
					self.load_file(
						pkg_dist, conf_dir, key + ".conf", val
					)

		except Exception as e:
			self.error = True
			self.logger.error(
				"Error occured while loading conf from distribution %s",
				repo_name, exc_info=e
			)


	def load_global_conf_from_repo(
		self, pkg_dist: pkg_resources.EggInfoDistribution, sub_dir: str = ""
	) -> None:
		""" """

		global_conf_path = sub_dir + "/ampel.conf" if sub_dir else "ampel.conf"

		try:

			# Global config
			if "ampel.conf" in pkg_dist.resource_listdir(sub_dir):

				if self.verbose:
					self.logger.verbose(
						"Loading %s from repo %s",
						global_conf_path, pkg_dist.project_name
					)

				self.load_general_conf(
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
				"Error while loading %s from repo %s",
				global_conf_path, pkg_dist.project_name, exc_info=e
			)


	def load_file(
		self, pkg_dist: pkg_resources.EggInfoDistribution,
		conf_dir: str, conf_file: str, func: Callable[[Dict], None]
	) -> None:
		""" """

		try:

			if conf_file in pkg_dist.resource_listdir(conf_dir):

				if self.verbose:
					self.logger.verbose(
						"Loading %s/%s from repo %s", conf_dir,
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
				"Error occured while loading %s/%s from repo %s",
				conf_dir, conf_file, pkg_dist.project_name, exc_info=e
			)


	def load_dir_files(
		self, pkg_dist: pkg_resources.EggInfoDistribution, conf_dir: str, 
		sub_dir_name: str, func: Callable[[Dict], None]
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
							"Loading %s/%s/%s from repo %s" % 
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
						"Error occured while loading %s/%s/%s from repo %s", conf_dir, 
						sub_dir_name, file_name, pkg_dist.project_name, exc_info=e
					)


	@staticmethod
	def get_repo_names(distrib_prefix: str="ampel-") -> List[str]:
		"""
		"""
		return [
			repo_name for repo_name in pkg_resources.AvailableDistributions() 
			if distrib_prefix in repo_name
		]	
