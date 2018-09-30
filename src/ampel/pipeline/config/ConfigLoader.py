#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ConfigLoader.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import partial
import sys, json, pkg_resources, traceback
from os.path import join, dirname, abspath, realpath
from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig
from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig

class ConfigLoader:

	DEFAULT_CONFIG = join(
		abspath(dirname(realpath(__file__))), 
		'ztf_config.json'
	)

	@staticmethod
	def load_config(path=DEFAULT_CONFIG, gather_plugins=True):
		"""
		Load the JSON configuration file at path
		and add plugins registered via pkg_resources
		"""
		try:
	
			with open(path) as f:
				config = json.load(f)
			if not gather_plugins:
				return config
	
			# Channels
			##########
	
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
								chan_dict['job'], resource.name, resource.dist,
								"already exists in the provided config file"
							)
						)
	
					# Schema validation
					try:
						ChannelConfig.parse_obj(chan_dict)
						config['channels'][chan_dict['channel']] = chan_dict
					except Exception as e:
						print("Error in {} from {}".format(resource.name, resource.dist))
						raise

			# T0 filters
			############
	
			for resource in pkg_resources.iter_entry_points('ampel.pipeline.t0'):
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
	
			# T3 Jobs
			#########
	
			for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.jobs'):
	
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
					try:
						T3JobConfig.parse_obj(job_dict)
						config['t3Jobs'][job_dict['job']] = job_dict
					except Exception as e:
						print("Error in {} from {}".format(resource.name, resource.dist))
						raise
	
		except Exception as e:
			print("Exception in load_config:")
			print("-"*60)
			traceback.print_exc(file=sys.stdout)
			print("-"*60)
			raise

		return config
