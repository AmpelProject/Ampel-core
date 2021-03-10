#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/DistConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, yaml, pkg_resources, os
from pkg_resources import EggInfoDistribution, DistInfoDistribution # type: ignore[attr-defined]
from typing import Dict, Any, Union, Callable, List, Optional, Generator
from ampel.config.builder.ConfigBuilder import ConfigBuilder
from ampel.log import VERBOSE


class DistConfigBuilder(ConfigBuilder):
	"""
	Subclass of ConfigLoader allowing to automatically detect and load
	various configuration files contained in ampel sub-packages installed with pip
	"""

	def load_distributions(self,
		prefixes: List[str] = ["pyampel-", "ampel-"],
		conf_dirs: List[str] = ["conf"],
		exts: List[str] = ["json", "yaml", "yml"]
	) -> None:
		"""
		:param prefixes: loads all known conf files from all distributions with name starting with prefixes
		:param conf_dirs: loads only conf files from these directories
		:param exts: loads only conf files with these extensions
		"""
		for prefix in prefixes:

			for dist_name in self.get_dist_names(prefix):

				if self.verbose:
					self.logger.log(VERBOSE, f"Checking distribution '{dist_name}'")

				for conf_dir in conf_dirs:
					for ext in exts:
						self.load_distrib(dist_name, conf_dir, ext)

				if self.verbose:
					self.logger.log(VERBOSE, f"Done checking distribution '{dist_name}'")

		if self.verbose:
			self.logger.log(VERBOSE, "Done loading distributions")


	def walk_pth_file(self, pth: str, conf_dir: str, ext: str) -> Generator[str, None, None]:
		with open(pth) as f:
			for root, dirs, files in os.walk(f"{f.read().strip()}/{conf_dir}"):
				for fname in files:
					if fname.endswith("."+ext):
						yield os.path.join(root, fname)
	

	def load_distrib(self,
		dist_name: str, conf_dir: str = "conf", ext: str = "json"
	) -> None:
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
				# 'conf/ampel-ztf.conf,sha256=FZkChNKKpcMPTO4pwyKq4WS8FAbznuR7oL9rtNYS7U0,322',
				# 'ampel/model/ZTFLegacyChannelTemplate.py,sha256=zVtv4Iry3FloofSazIFc4h8l6hhV-wpIFbX3fOW2njA,2182',
				# 'ampel/model/__pycache__/ZTFLegacyChannelTemplate.cpython-38.pyc,,',
				if pth := next(
					(
						pth for el in distrib.get_metadata_lines('RECORD')
						if (pth := el.split(",")[0]).endswith(".pth")
					),
					None
				):
					all_conf_files = list(self.walk_pth_file(pth, conf_dir, ext))
				else:
					all_conf_files = [
						el.split(",")[0] for el in distrib.get_metadata_lines('RECORD')
						if el.startswith(conf_dir) and f".{ext}," in el
					]	

			elif isinstance(distrib, EggInfoDistribution):
				# Example of the ouput of distrib.get_metadata_lines('SOURCES.txt'):
				# 'setup.py',
				# 'conf/ampel-ztf.json',
				# 'ampel/model/ZTFLegacyChannelTemplate.py',
				all_conf_files = [
					el for el in distrib.get_metadata_lines('SOURCES.txt')
					if (el.startswith(f"{conf_dir}") and el.endswith(f".{ext}"))
				]

			else:
				raise ValueError(f"Unsupported distribution type: '{type(distrib)}'")

			if all_conf_files and self.verbose:
				self.logger.log(VERBOSE, f"Following conf files will be parsed: {all_conf_files}")

			if ampel_conf := self.get_conf_file(all_conf_files, f"ampel.{ext}"):
				self.load_conf_using_func(distrib, ampel_conf, self.load_ampel_conf) # type: ignore

			# Channel, db (and template) can be defined by multiple files
			# in a directory named after the corresponding config section name
			for key in ("channel", "db", "process"):
				if specialized_conf_files := self.get_conf_files(all_conf_files, f"/{key}/"):
					for f in specialized_conf_files:
						self.load_conf_using_func(distrib, f, self.first_pass_config[key].add)

			# ("channel", "db", "resource")
			for key in self.first_pass_config.conf_keys.keys():
				if key == "alias":
					continue
				if section_conf_file := self.get_conf_file(all_conf_files, f"{key}.{ext}"):
					self.load_conf_using_func(distrib, section_conf_file, self.first_pass_config[key].add) # type: ignore

			# ("controller", "processor", "unit", "alias", "process")
			for unit_type in ("alias", "process"):
				if tier_conf_file := self.get_conf_file(all_conf_files, f"{unit_type}.{ext}"):
					self.load_tier_config_file(distrib, tier_conf_file, unit_type) # type: ignore

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


	def load_conf_using_func(self,
		distrib: Union[EggInfoDistribution, DistInfoDistribution],
		file_rel_path: str,
		func: Callable[[Dict[str, Any], str, str], None]
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
				file_rel_path,
				distrib.project_name
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
		distrib: Union[EggInfoDistribution, DistInfoDistribution],
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
						d[k], file_name=file_rel_path,
						dist_name=distrib.project_name
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
	def get_dist_names(distrib_prefix: str = "ampel-") -> List[str]:
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
