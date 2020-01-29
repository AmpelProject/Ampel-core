#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/DistConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
import pkg_resources
from pkg_resources import EggInfoDistribution, DistInfoDistribution # type: ignore
from typing import Dict, Any, Union, Callable, List, Optional
from ampel.config.builder.ConfigBuilder import ConfigBuilder


class DistConfigBuilder(ConfigBuilder):
	"""
	Subclass of ConfigLoader allowing to automatically detect and load 
	various configuration files contained in ampel sub-packages installed with pip 
	"""

	def load_distributions(self, prefix: str = "ampel-", conf_dir: str = "conf") -> None:
		"""
		Loads all known conf files from all distributions with name starting with ampel-
		"""
		for distrib_name in self.get_dist_names(prefix):

			if self.verbose:
				self.logger.verbose("#"*150)
				self.logger.verbose(f"Checking distribution '{distrib_name}'")

			self.load_distrib(distrib_name, conf_dir)


	def load_distrib(self, dist_name: str, conf_dir: str = "conf") -> None:
		""" 
		Loads all known conf files of the provided distribution (name)
		"""
		try:

			if not conf_dir.endswith("/"):
				conf_dir += "/"

			distrib = pkg_resources.get_distribution(dist_name)

			# DistInfoDistribution: look for metadata RECORD (in <dist_name>.dist-info)
			if isinstance(distrib, DistInfoDistribution):
				# Example of the ouput of distrib.get_metadata_lines('RECORD'):
 				# 'ampel-conf/ampel-ztf.conf,sha256=FZkChNKKpcMPTO4pwyKq4WS8FAbznuR7oL9rtNYS7U0,322',
 				# 'ampel/model/ZTFLegacyChannelTemplate.py,sha256=zVtv4Iry3FloofSazIFc4h8l6hhV-wpIFbX3fOW2njA,2182',
 				# 'ampel/model/__pycache__/ZTFLegacyChannelTemplate.cpython-38.pyc,,',
				all_conf_files = [
					el.split(",")[0] for el in distrib.get_metadata_lines('RECORD') 
					if el.startswith(conf_dir) and ".conf," in el
				]

			elif isinstance(distrib, EggInfoDistribution):
				# Example of the ouput of distrib.get_metadata_lines('SOURCES.txt'):
 				# 'setup.py',
 				# 'ampel-conf/ampel-ztf.conf',
 				# 'ampel/model/ZTFLegacyChannelTemplate.py',
				all_conf_files = [
					el for el in distrib.get_metadata_lines('SOURCES.txt') 
					if (el.startswith("ampel-conf/") and el.endswith(".conf"))
				]

			else:
				raise ValueError(f"Unsupported distribution type: '{type(distrib)}'")

			if ampel_conf := self.get_conf_file(all_conf_files, "ampel.conf"):
				self.load_conf_using_func(distrib, ampel_conf, self.load_ampel_conf) # type: ignore

			# Channel, db (and template) can be defined by multiple files
			# in a directory named after the corresponding config section name
			for key in ("channel", "db"):
				if specialized_conf_files := self.get_conf_files(all_conf_files, f"/{key}/"):
					for f in specialized_conf_files:
						self.load_conf_using_func(distrib, f, self.base_config[key].add)

			# ("channel", "db", "resource")
			for key in self.base_config.general_keys.keys():
				if section_conf_file := self.get_conf_file(all_conf_files, f"{key}.conf"):
					self.load_conf_using_func(distrib, section_conf_file, self.base_config[key].add) # type: ignore

			# ("controller", "processor", "unit", "alias", "process")
			for category in self.base_config.tier_keys.keys():
				if tier_conf_file := self.get_conf_file(all_conf_files, f"{category}.conf"):
					self.load_tier_config_file(distrib, tier_conf_file, category) # type: ignore

			# Try to load templates from folder template (defined by 'Ampel-ZTF' for ex.)

			if template_conf_files := self.get_conf_files(all_conf_files, "/template/"):
				for f in template_conf_files:
					self.load_conf_using_func(distrib, f, self.register_channel_templates)

			# Try to load templates from template.conf
			if template_conf := self.get_conf_file(all_conf_files, "/template.conf"):
				self.load_conf_using_func(distrib, template_conf, self.register_channel_templates) # type: ignore

			if all_conf_files:
				self.logger.info(
					f"Not all conf files were loaded from distribution '{distrib.project_name}'\n"
					f"Unprocessed conf files: {all_conf_files}"
				)

		except Exception as e:
			self.error = True
			self.logger.error(
				f"Error occured while loading conf from the distribution '{dist_name}'",
				exc_info=e
			)


	def load_conf_using_func(self, 
		distrib: Union[EggInfoDistribution, DistInfoDistribution], 
		file_rel_path: str, 
		func: Callable[[Dict[str, Any], str], None]
	) -> None:
		""" """

		try:

			if self.verbose:
				self.logger.verbose(
					f"Loading {file_rel_path} from distribution '{distrib.project_name}'"
				)

			func(
				json.loads(
					distrib.get_resource_string(__name__, file_rel_path)
				),
				distrib.project_name
			)

		except Exception as e:
			self.error = True
			self.logger.error(
				f"Error occured loading {file_rel_path} from distribution '{distrib.project_name}'",
				distrib.project_name, exc_info=e
			)


	def load_tier_config_file(self, 
		distrib: Union[EggInfoDistribution, DistInfoDistribution], 
		file_rel_path: str, 
		category: str
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
				self.logger.verbose(
					f"Loading {file_rel_path} from distribution '{distrib.project_name}'"
				)

			d = json.loads(
				distrib.get_resource_string(__name__, file_rel_path)
			)

			for k in ("t0", "t1", "t2", "t3"):
				if k in d:
					self.base_config[k][category].add(
						d[k], distrib.project_name
					)

		except Exception as e:
			self.error = True
			self.logger.error(
				f"Error occured loading {file_rel_path} from distribution '{distrib.project_name}'",
				exc_info=e
			)


	@staticmethod
	def get_dist_names(distrib_prefix: str="ampel-") -> List[str]:
		""" 
		Get all installed distributions whose names start with the provided prefix
		"""
		return [
			dist_name for dist_name in pkg_resources.AvailableDistributions() # type: ignore
			if distrib_prefix in dist_name
		]


	@staticmethod
	def get_conf_file(files: List[str], key: str) -> Optional[str]:
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
	def get_conf_files(files: List[str], key: str) -> List[str]:
		"""
		Extract all entries who matches the provided 'key' from the provided list
		Note: this method purposely modifies the input list (removes matched elements)
		"""
		ret = [el for el in files if key in el]
		for el in ret:
			files.remove(el)
		return ret
