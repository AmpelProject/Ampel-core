#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/ConfigBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 09.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources, re
from typing import Dict, Callable, List, Union, Sequence, Any

from ampel.db.DBUtils import DBUtils
from ampel.common.AmpelUtils import AmpelUtils
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.config.builder.T3ConfigMorpher import T3ConfigMorpher


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

		self.func_load_file = {
			"unit": lambda x, y: self.add_units("unit", x, y),
			"executor": lambda x, y: self.add_units("executor", x, y),
			"controller": lambda x, y: self.add_units("controller", x, y),
			"alias": self.add_aliases,
			"resource": self.add_resources
		}

		self._config = {
			"db": {"prefix": "Ampel", "options": {"hashChanNames": True}},
			"t0": {"process": [], "alias": {}},
			"t1": {"process": {}, "alias": {}},
			"t2": {"process": {}, "alias": {}, "runConfig": {}},
			"t3": {"process": {}, "alias": {}, "job": {}},
			"channel": {},
			"unit": {},
			"executor": {},
			"controller": {},
			"resource": {},
			"pwd": []
		}


	def add_passwords(self, arg: Union[str, List[str]]) -> None:
		""" """
		# Not set() is not directly json encodable
		for el in AmpelUtils.iter(arg):
			if el not in self._config['pwd']:
				self._config['pwd'].append(el)


	def load_general_conf(self, arg: Dict, repo_name: str="undefined") -> None:
		""" """
		if 'channel' in arg:
			if not isinstance(arg['channel'], Sequence):
				raise ValueError("General conf error: channel value type must be a sequence")
			self.load_from_dict(arg, 'channel', self.add_channels, repo_name)

		if 'job' in arg:
			if not isinstance(arg['job'], Sequence):
				raise ValueError("General conf error: channel value type must be a sequence")
			self.load_from_dict(arg, 'job', self.add_job, repo_name)

		for key, val in self.func_load_file.items():
			if key in arg:
				val(arg[key], repo_name)


	def load_from_dict(
		self, conf: Dict, key_name: str, func: Callable[[Dict], None], repo_name: str="undefined"
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


	def add_channels(
		self, arg: Union[Dict[str, Any], List[Dict[str, Any]]], repo_name: str="Undefined"
	) -> None:
		""" """

		if isinstance(arg, dict):
			arg = (arg, )

		for chan_dict in arg:

			if "channel" not in chan_dict:
				self.logger.error(
					"Channel dict is missing key 'channel' (repo %s)" % repo_name
				)
				self.error = True
				return

			try:

				chan_name = chan_dict.pop('channel')

				if self.verbose:
					self.logger.verbose("-> Loading channel: " + chan_name)

				chan_dict['repo'] = repo_name

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
					chan_dict['hashedName'] = DBUtils.b2_hash(chan_name)
				elif type(chan_name) is int:
					chan_dict['hashedName'] = chan_name
				else:
					self.logger.error(
						"Channel name must be of type str or int (chan: %s, repo %s)" % 
						(chan_name ,repo_name)
					)
					self.error = True
					return

				for i, process in enumerate(AmpelUtils.iter(chan_dict['process'])):

					if process.get("tier", None) == 3:

						conf = T3ConfigMorpher(process).get_conf(chan_name, i)
						process_name = conf["processName"]
						if process_name in self._config['t3']['process']:
							self.logger.error(
								"Duplicated process name: %s %s", process_name, 
								self.get_collision_help_msg(
									self._config['t3']['process'][process_name]['repo'],
									repo_name
								)
							)
							self.error = True
							return

						tasks = AmpelUtils.get_by_path(conf, "executor.runConfig.task")

						if tasks: 

							if not getattr(self, "tmp_tasks"):
								self.tmp_tasks = {}

							for task in tasks:

								task['repo'] = repo_name
								task_name = task['taskName']

								if task_name in self.tmp_tasks:
									self.logger.error(
										"Duplicated task name: %s %s", task_name, 
										self.get_collision_help_msg(
											self.tmp_tasks['t3'][task_name]['repo'],
											repo_name
										)
									)
									self.error = True
									return

						self._config['t3']['process'][process_name] = process
						del chan_dict['process'][i]

				self._config['channel'][chan_name] = chan_dict

			except Exception as e:
				self.error = True
				self.logger.error(
					"Error occured while loading channel config (repo: %s). Offending value: %s" % 
					(repo_name, chan_dict), exc_info=e
				)


	def add_aliases(self, arg: Dict, repo_name: str=None) -> None:
		""" 
		"""
		for tier in arg:

			if tier not in ("t0", "t1", "t2", "t3"):
				self.logger.error(
					"Alias error: ignoring unknown key: %s (repo: %s)" % 
					(tier, repo_name)
				)
				self.error = True
				continue

			if not isinstance(arg[tier], dict):
				self.logger.error(
					"Alias error: provided value must be have dict type (key: %s, repo: %s)" % 
					(tier, repo_name)
				)
				self.error = True
				continue

			for key in arg[tier]:

				try:

					if self.verbose:
						self.logger.verbose("-> Loading alias: " + key)

					if repo_name:
						if repo_name not in self._config[tier]['alias']:
							d = {}
							self._config[tier]['alias'][repo_name] = d
						else:
							d = self._config[tier]['alias'][repo_name]
					else:
						d = self._config[tier]['alias']

					if key in d and (
						AmpelUtils.build_unsafe_dict_id(arg[tier][key]) == \
						AmpelUtils.build_unsafe_dict_id(d[key])
					):
						self.logger.error("Duplicated alias: %s.%s" % (tier, key))
						self.error = True
						return

					d[key] = arg[tier][key]

				except Exception as e:
					self.error = True
					self.logger.error(
						"Error occured while loading alias %s.%s" %
						(tier, key), exc_info=e
					)


	def add_units(self, config_root_key: str, arg: List[str], repo_name: str="Undefined") -> None:
		""" """
		for el in arg:

			try:

				k = re.sub(".*\.", "", el)

				if k in self._config[config_root_key]:
					self.logger.error('Duplicated ampel %s: %s' % (config_root_key, k))
					self.error = True
					return

				self._config[config_root_key][k] = {
					'fqn': el,
					'repo': repo_name
				}

				if self.verbose:
					self.logger.verbose("-> Loading %s: %s" + (config_root_key, k))

			except Exception as e:
				self.error = True
				self.logger.error(
					"Error occured while loading %s: %s" % 
					(config_root_key, el), exc_info=e
				)


	def add_resources(self, arg: Dict, repo_name: str="Undefined") -> None:
		""" 
		"""
		for key in arg:

			try:
				if self.verbose:
					self.logger.verbose("-> Loading resource: " + key)

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


	def add_job(self, arg: Dict, repo_name: str="Undefined") -> None:
		""" """

		if "job" not in arg:
			self.logger.error("Job dict is missing key 'job'")
			self.error = True
			return

		job_name = arg["job"]
		arg['repo'] = repo_name

		if self.verbose:
			self.logger.verbose("-> Loading job: " + job_name)

		if job_name in self._config['t3']['job']:
			self.logger.error("Duplicated job: '%s'", job_name)
			self.error = True
			return

		self._config['t3']['job'][job_name] = arg


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

			for el in AmpelUtils.iter(self._config['channel'][channel]['process']):

				# We could not do the following on the fly previously (in add_channel(...))
				# because some aliases might not have been added at the time
				if "t2Compute" in el:

					for t2 in AmpelUtils.iter(el['t2Compute']):

						rc = t2.get('runConfig', None)

						if not rc:
							continue

						if isinstance(rc, str):

							repo_name = self._config['channel'][channel]['repo']

							if (
								repo_name not in self._config['t2']['alias'] or 
								rc not in self._config['t2']['alias'][repo_name]
							):
								self.logger.error(
									"Error in channel %s (repo %s)", 
									channel, self._config['channel'][channel]['repo']
								)
								raise ValueError(
									"Unknown T2 run config alias defined in channel %s:\n %s" % 
									(channel, t2)
								)

							rc = self._config['t2']['alias'][repo_name][rc]

						if isinstance(rc, Dict):
							override = t2.get('override', None)
							if override:
								rc = {**rc, **override}
							b2_hash = DBUtils.b2_dict_hash(rc)
							t2['runConfig'] = b2_hash
							self._config['t2']['runConfig'][b2_hash] = rc
							continue

						if isinstance(rc, int):
							if rc in self._config['t2']['runConfig']:
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


	def do_not_hash_chan_names(self) -> None:
		""" Not recommended """
		self._config['db']['options']["hashChanNames"] = False


	@staticmethod
	def get_collision_help_msg(src_def: str, new_def: str) -> str:
		"""
		"""
		if src_def == "Undefined":
			return ""

		if src_def == new_def:
			return "(collision originates from different definitions in the same repository %s)" % src_def

		return "(initialy defined by repository %s)" % src_def
