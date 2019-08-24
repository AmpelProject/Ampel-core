#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/ConfigLoader.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 24.08.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources, traceback
import logging
from os.path import join, dirname, abspath, realpath, exists
from ampel.config.channel.ChannelConfig import ChannelConfig
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.t3.T3JobConfig import T3JobConfig
from ampel.logging.AmpelLogger import AmpelLogger

log = logging.getLogger(__name__)

class ConfigLoader:

	DEFAULT_CONFIG = join(
		abspath(dirname(realpath(__file__))), 
		'ampel_config.json'
	)

	BASE_CONFIG = {
		"AmpelDB" : {
		  "prefix": "Ampel"
		},
		"resources": {},
		"channels": {},
		"t0Units": {},
		"t0Filters": {},
		"t1Units": {},
		"t2Units": {},
		"t2RunConfig": {},
		"t3Units": {},
		"t3Jobs": {},
		"t3Tasks": {},
		"pwds": []
	}


	@staticmethod
	def load_config(path=DEFAULT_CONFIG, tier="all", gather_plugins=True):
		"""
		Load the JSON configuration from the string path (or the file at that path)
		and add plugins registered via pkg_resources

		:param tier: possible values are: 'all', 0, 1, 2, 3.
		Loads configuration by taking the given scope restriction into account.
		Less checks are performed when restricting config loading to a given tier,
		which yields quicker procedure. For example, with tier set to 3, 
		T0/T2 units and run configurations are not loaded/checked.
		"""

		config = ConfigLoader.BASE_CONFIG.copy()

		if exists(path):
			with open(path) as f:
				config.update(json.load(f))
		else:
			config.update(json.loads(path))

		if not gather_plugins:
			return config


		# Channels
		##########

		if tier in ("all", 0, 3):

			from ampel.db.DBUtils import DBUtils

			for resource in pkg_resources.iter_entry_points('ampel.channels'):

				# Channel can be provided as single dict or list of dicts
				chan_resource = resource.resolve()()

				if type(chan_resource) is dict:
					chan_resource = [chan_resource]
	
				for chan_dict in chan_resource:

					# Check duplicated channel names
					if chan_dict['channel'] in config['channels']:
						raise KeyError(
							"Channel {} (defined as entry point {} in {}) {}".format(
								chan_dict['channel'], resource.name, resource.dist,
								"already exists in the provided config file"
							)
						)
	
					# Schema validation
					try:
						ChannelConfig.create(tier, **chan_dict)
						chan_dict["b2hash"] = DBUtils.b2_hash(chan_dict['channel'])
						config['channels'][chan_dict['channel']] = chan_dict
					except Exception as e:
						AmpelLogger.get_logger().error(
							"Error in {} from {}".format(
								resource.name, resource.dist
							)
						)
						raise


		# T0 units & filters
		####################

		if tier in ("all", 0):

			for resource in pkg_resources.iter_entry_points('ampel.t0.units'):

				klass = resource.resolve()
				name = resource.name

				if name in config['t0Filters']:
					raise KeyError(
						"{} (defined as entry point {} in {}) {}".format(
							name, resource.name, resource.dist,
							"already exists in the provided config file"
						)
					)

				config['t0Filters'][name] = dict(classFullPath=klass.__module__)


		# T2 RunConfigs
		###############

		if tier in ("all", 2):

			for resource in pkg_resources.iter_entry_points('ampel.t2.configs'):

				for name, t2config in resource.resolve()().items():

					if name in config['t2RunConfig']:
						raise KeyError(
							"T2 run config {} (defined as entry point {} in {}) {}".format(
								name, resource.name, resource.dist,
								"already exists in the provided config file"
							)
						)

					if name != '{t2Unit}_{runConfig}'.format(**t2config):
						raise KeyError(
							"T2 run config {} (defined as entry point {} in {}) {}".format(
								name, resource.name, resource.dist,
								"should be named {t2Unit}_{runConfig}".format(**t2config)
							)
						)

					config['t2RunConfig'][name] = t2config


		# T3 Jobs and Tasks
		###################

		if tier in ("all", 3):

			# T3 Jobs 
			#########

			for resource in pkg_resources.iter_entry_points('ampel.t3.jobs'):

				try:
					# T3 job can be provided as single dict
					job_resource = resource.resolve()()
					if type(job_resource) is dict:
						job_resource = [job_resource]
	
					for job_dict in job_resource:

						# Check duplicated job names
						if job_dict['job'] in config['t3Jobs']:
							raise KeyError(
								"T3 job {} (defined as entry point {} in {}) {}".format(
									job_dict['job'], resource.name, resource.dist,
									"already exists in the provided config file"
								)
							)
						# Schema validation
						T3JobConfig(**job_dict)
						config['t3Jobs'][job_dict['job']] = job_dict

				except Exception as e:
					AmpelLogger.get_logger().error(
						"Error in {} from {}".format(
							resource.name, resource.dist
						)
					)
					raise

			# T3 Tasks
			##########

			for resource in pkg_resources.iter_entry_points('ampel.channels'):

				# Channel can be provided as single dict or list of dicts
				chan_resource = resource.resolve()()

				if type(chan_resource) is dict:
					chan_resource = [chan_resource]
	
				for chan_dict in chan_resource:

					# Schema validation
					try:

						cc = ChannelConfig.create(3, **chan_dict)

						for source in cc.sources:

							if not hasattr(source, 't3Supervise') or not cc.active:
								continue
						
							for t3_task in getattr(source, 't3Supervise', []):
				
								# Check duplicated task names
								if t3_task.get('task') in config['t3Tasks']:
									raise KeyError(
										"Task {} (defined as entry point {} in {}) {}".format(
											t3_task.get('task'), resource.name, resource.dist,
											"already exists in the provided config file"
										)
									)

								config['t3Tasks'][t3_task.get('task')] = t3_task.dict()

					except Exception as e:
						AmpelLogger.get_logger().error(
							"Error in {} from {}".format(
								resource.name, resource.dist
							)
						)
						raise
	
		return config
