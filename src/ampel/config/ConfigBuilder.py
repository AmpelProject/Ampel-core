#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/ConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 04.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources, re
from typing import Dict, Callable, List, Union
from ampel.db.DBUtils import DBUtils
from ampel.common.AmpelUtils import AmpelUtils
from ampel.logging.AmpelLogger import AmpelLogger


class ConfigBuilder:
	"""
	Builds a central configuration dict for ampel.
	Typically, the output of this class (method get_config())
	is used as input for the method set_config() of an AmpelConfig instance.
	This class can automcatically detect and load various configuration files 
	contained in ampel sub-packages installed with pip [see the methods
	load_distributions(...), load_distrib(...) or load_global_conf_from_repo(...)]
	Manual loading of custom / self loaded dict is also possible.
	The method add_passwords(...) allows to add a list of passwords used to later
	decrypt encrypted config entries.
	"""

	def __init__(self, verbose: bool=False, logger=None):
		""" """
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.verbose = verbose
		self.error = False

		self._config = {
			"db": {"prefix": "Ampel"},
			"channel": {},
			"unit": {},
			"alias": {},
			"runConfig": {"t2": {}}, # internal use only
			"resource": {},
			"job": {},
			"task": {},
			"instrument": {},
			"pwd": []
		}


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
					pkg_dist, conf_dir, "channels", self.load_channel
				)

				self.load_dir_files(
					pkg_dist, conf_dir, "jobs", self.load_job
				)

				self.load_file(
					pkg_dist, conf_dir, "unit.conf", self.load_units
				)

				self.load_file(
					pkg_dist, conf_dir, "alias.conf", self.load_aliases
				)

				self.load_file(
					pkg_dist, conf_dir, "resource.conf", self.load_resources
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


	def add_passwords(self, arg: Union[str, List[str]]) -> None:
		""" """
		# Not set() is not directly json encodable
		for el in AmpelUtils.iter(arg):
			if el not in self._config['pwd']:
				self._config['pwd'].append(el)


	def load_general_conf(self, arg: Dict, repo_name: str="undefined") -> None:
		""" """
		if 'channel' in arg:
			self.load_from_dict(arg, 'channel', self.load_channel, repo_name)

		if 'job' in arg:
			self.load_from_dict(arg, 'job', self.load_job, repo_name)

		if 'unit' in arg:
			self.load_units(arg['unit'], repo_name)

		if 'alias' in arg:
			self.load_aliases(arg['alias'], repo_name)

		if 'resource' in arg:
			self.load_resources(arg['resource'], repo_name)


	def load_from_dict(
		self, conf: Dict, key_name: str, func: Callable[[Dict], None], 
		repo_name: str="undefined"
	) -> None:
		""" """

		value = conf[key_name]

		# Be flexible
		if isinstance(value, Dict):
			func(value, repo_name)

		elif isinstance(value, list):
			for chan in value:
				func(chan, repo_name)
		else:
			self.logger.error(
				"Value must be a of type Dict or List (Dict key %s)" % key_name
			)


	def load_channel(self, arg: Dict, repo_name: str="Undefined") -> None:
		""" """

		if "channel" not in arg:
			self.logger.error("Channel dict is missing key 'channel'")
			self.error = True
			return

		try:

			chan_name = arg.pop('channel')

			if self.verbose:
				self.logger.verbose("-> Loading channel " + chan_name)

			arg['repo'] = repo_name

			# Check duplicated channel names
			if chan_name in self._config['channel']:
				self.logger.error(
					"Duplicated channel name: '%s' %s", chan_name, 
					self.get_collision_help_msg(
						self._config['channel'][chan_name]['repo'],
						repo_name
					)
				)
				self.error = True
				return

			if type(chan_name) is str:
				arg['hash'] = DBUtils.b2_hash(chan_name)
			elif type(chan_name) is int:
				arg['hash'] = chan_name
			else:
				self.logger.error("Channel name must be of type str or int")
				self.error = True
				return

			for el in AmpelUtils.iter(arg['sources']):

				if "t3Supervise" in el:

					for task in AmpelUtils.iter(el.pop('t3Supervise')):

						task['transients']['select']['channel'] = arg['hash']
						task['repo'] = repo_name
						task_name = task['task']

						if task_name in self._config['task']:
							self.logger.error(
								"Duplicated task name: %s %s", task_name, 
								self.get_collision_help_msg(
									self._config['task'][task_name]['repo'],
									repo_name
								)
							)
							self.error = True
							return

						self._config['task'][task_name] = task

			self._config['channel'][chan_name] = arg

		except Exception as e:
			self.error = True
			self.logger.error(
				"Error occured while loading channel config", exc_info=e
			)


	def load_aliases(self, arg: Dict, repo_name: str=None) -> None:
		""" 
		"""
		for key in arg:

			try:
				if self.verbose:
					self.logger.verbose("-> Loading alias " + key)

				if repo_name:
					if repo_name not in self._config['alias']:
						d = {}
						self._config['alias'][repo_name] = d
					else:
						d = self._config['alias'][repo_name]
				else:
					d = self._config['alias']

				if key in d and (
					AmpelUtils.build_unsafe_dict_id(arg[key]) == \
					AmpelUtils.build_unsafe_dict_id(d[key])
				):
					self.logger.error("Duplicated alias: " + key)
					self.error = True
					return

				d[key] = arg[key]

			except Exception as e:
				self.error = True
				self.logger.error(
					"Error occured while loading alias " + key, exc_info=e
				)


	def load_units(self, arg: List[str], repo_name: str="Undefined") -> None:
		""" """
		for el in arg:

			try:

				k = re.sub(".*\.", "", el)

				if k in self._config['unit']:
					self.logger.error('Duplicated ampel unit: ' % k)
					self.error = True
					return

				self._config['unit'][k] = {
					'fqn': el,
					'repo': repo_name
				}

				if self.verbose:
					self.logger.verbose("-> Loading unit " + k)

			except Exception as e:
				self.error = True
				self.logger.error(
					"Error occured while loading unit " + el, exc_info=e
				)


	def load_resources(self, arg: Dict, repo_name: str="Undefined") -> None:
		""" 
		"""
		for key in arg:

			try:
				if self.verbose:
					self.logger.verbose("-> Loading resource " + key)

				if repo_name not in self._config['resource']:
					self._config['resource'][repo_name] = {}
				else:
					if key in self._config['resource'][repo_name]:
						self.logger.error("Duplicated resource: " + key)
						self.error = True
						return

				self._config['resource'][repo_name][key] = arg[key]

			except Exception as e:
				self.error = True
				self.logger.error(
					"Error occured while loading resource " + key, exc_info=e
				)


	def load_job(self, arg: Dict, repo_name: str="Undefined") -> None:
		""" """

		if "job" not in arg:
			self.logger.error("Job dict is missing key 'job'")
			self.error = True
			return

		job_name = arg["job"]
		arg['repo'] = repo_name

		if self.verbose:
			self.logger.verbose("-> Loading job " + job_name)

		if job_name in self._config['job']:
			self.logger.error("Duplicated job: '%s'", job_name)
			self.error = True
			return

		self._config['job'][job_name] = arg


	def get_config(self, ignore_errors: bool=False) -> Dict:
		""" 
		:raises: ValueError if self.error is True - this behavior can be disabled using the parameter ignore_errors
		"""
		if self.error:
			if not ignore_errors:
				raise ValueError(
					"Error occured while building config, you can use the option ignore_errors=True \n" +
					"to bypass this exception and get the (possibly non-working) config nonetheless"
				)

		for channel in self._config['channel'].keys():

			for el in AmpelUtils.iter(self._config['channel'][channel]['sources']):

				# We could not do the following on the fly previously (in load_channel(...))
				# because some aliases might not have been added at the time
				if "t2Compute" in el:

					for t2 in AmpelUtils.iter(el['t2Compute']):

						rc = t2.get('runConfig', None)

						if not rc:
							continue

						if isinstance(rc, str):

							repo_name = self._config['channel'][channel]['repo']

							if (
								repo_name not in self._config['alias'] or 
								rc not in self._config['alias'][repo_name]
							):
								self.logger.error(
									"Error in channel %s (defined in repo %s)", 
									channel, self._config['channel'][channel]['repo']
								)
								raise ValueError(
									"Unknown T2 run config alias defined in channel %s:\n %s" % 
									(channel, t2)
								)

							rc = self._config['alias'][repo_name][rc]

						if isinstance(rc, Dict):
							override = t2.get('override', None)
							if override:
								rc = {**rc, **override}
							b2_hash = DBUtils.b2_dict_hash(rc)
							t2['runConfig'] = b2_hash
							self._config['runConfig']['t2'][b2_hash] = rc
							continue

						if isinstance(rc, int):
							if rc in self._config['runConfig']['t2']:
								continue
							raise ValueError(
								"Unknown T2 run config alias defined in channel %s:\n %s" %
								(channel, t2)
							)

						raise ValueError(
							"Invalid T2 run config defined in channel %s:\n %s" %
							(channel, t2)
						)

		return self._config

	
	def print(self, ignore_errors: bool=False) -> None:
		"""
		"""
		print(
			json.dumps(
				self.get_config(ignore_errors), 
				indent=4
			)
		)


	@staticmethod
	def get_repo_names(distrib_prefix: str="ampel-") -> List[str]:
		"""
		"""
		return [
			repo_name for repo_name in pkg_resources.AvailableDistributions() 
			if distrib_prefix in repo_name
		]	


	@staticmethod
	def get_collision_help_msg(src_def: str, new_def: str) -> str:
		"""
		"""
		if src_def == "Undefined":
			return ""

		if src_def == new_def:
			return "(collision originates from different definitions in the same repository %s)" % src_def

		return "(initialy defined by repository %s)" % src_def
