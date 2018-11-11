#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ConfigLoader.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 11.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources, traceback
from os.path import join, dirname, abspath, realpath
from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig
from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig

class ConfigLoader:

	DEFAULT_CONFIG = join(
		abspath(dirname(realpath(__file__))), 
		'ztf_config.json'
	)

	@staticmethod
	def load_config(path=DEFAULT_CONFIG, tier="all", gather_plugins=True):
		"""
		Load the JSON configuration file at path
		and add plugins registered via pkg_resources

		:param tier: possible values are: 'all', 0, 1, 2, 3.
		Loads configuration by taking the given scope restriction into account.
		Less checks are performed when restricting config loading to a given tier,
		which yields quicker procedure. For example, with tier set to 3, 
		T0/T2 units and run configurations are not loaded/checked.
		"""

		try:
	
			with open(path) as f:
				config = json.load(f)
			if not gather_plugins:
				return config
	

			# Channels
			##########

			if tier in ("all", 0, 3):
	
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
							config['channels'][chan_dict['channel']] = chan_dict
						except Exception as e:
							print("Error in {} from {}".format(resource.name, resource.dist))
							raise
	

			# T0 filters
			############
	
			if tier in ("all", 0):

				for resource in pkg_resources.iter_entry_points('ampel.pipeline.t0.units'):
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

				for resource in pkg_resources.iter_entry_points('ampel.pipeline.t2.configs'):
					for name, channel_config in resource.resolve()().items():
						if name in config['t2RunConfig']:
							raise KeyError(
								"T2 run config {} (defined as entry point {} in {}) {}".format(
									name, resource.name, resource.dist,
									"already exists in the provided config file"
								)
							)
						config['t2RunConfig'][name] = channel_config
	
			# T3 Jobs and Tasks
			###################

			if tier in ("all", 3):

				# T3 Jobs 
				#########
	
				for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.jobs'):
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
						print("Error in {} from {}".format(resource.name, resource.dist))
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

								if not hasattr(source, 't3Supervise'):
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

									config['t3Tasks'][t3_task.get('task')] = t3_task.json()

						except Exception as e:
							print("Error in {} from {}".format(resource.name, resource.dist))
							raise
	

	
		except Exception as e:

			import sys
			print("Exception in load_config:")
			print("-"*60)
			traceback.print_exc(file=sys.stdout)
			print("-"*60)
			raise

		return config
