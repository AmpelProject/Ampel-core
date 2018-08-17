#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from functools import partial
import sys, inspect, json, pkg_resources, logging, traceback
from os.path import basename, dirname, abspath, realpath

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.t3.T3JobConfig import T3JobConfig

log = logging.getLogger(__name__)

DEFAULT_CONFIG = abspath(dirname(realpath(__file__)) + '/../../../../config/ztf_config.json')

def load_config(path=DEFAULT_CONFIG, gather_plugins=True):
	"""Load the JSON configuration file at path, and add plugins registered via pkg_resources"""
	try:
		with open(path) as f:
			config = json.load(f)
		if not gather_plugins:
			return config
		for resource in pkg_resources.iter_entry_points('ampel.channels'):
			for name, channel_config in resource.resolve()().items():
				if name in config['channels']:
					raise KeyError("Channel config {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
				config['channels'][name] = channel_config
		for resource in pkg_resources.iter_entry_points('ampel.pipeline.t0'):
			klass = resource.resolve()
			name = resource.name
			if name in config['t0_filters']:
				raise KeyError("{} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
			config['t0_filters'][name] = dict(classFullPath=klass.__module__)
		for resource in pkg_resources.iter_entry_points('ampel.pipeline.t2.configs'):
			for name, channel_config in resource.resolve()().items():
				if name in config['t2_run_config']:
					raise KeyError("T2 run config {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
				config['t2_run_config'][name] = channel_config
		for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.configs'):
			for name, channel_config in resource.resolve()().items():
				if name in config['t3_run_config']:
					raise KeyError("T3 run config {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
				config['t3_run_config'][name] = channel_config
		for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.jobs'):
			for name, channel_config in resource.resolve()().items():
				if name in config['t3_jobs']:
					raise KeyError("T3 job {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
				try:
					T3JobConfig.job_schema(channel_config)
				except Exception as e:
					print("Error in {} from {}".format(name, resource.dist))
					raise
				config['t3_jobs'][name] = channel_config
	except Exception as e:
		print("Exception in load_config:")
		print("-"*60)
		traceback.print_exc(file=sys.stdout)
		print("-"*60)
		raise
	return config

from configargparse import ArgumentParser, Namespace, Action
import os

class file_environ(dict):
	"""
	Facade for os.environ that reads the value of environment variable FOO from
	the the file pointed to by the variable FOO_FILE.
	"""
	def __init__(self):
		super(file_environ, self).__init__(os.environ)
	def __contains__(self, key):
		if dict.__contains__(self, key):
			return True
		elif isinstance(key, str):
			fkey = '{}_FILE'.format(key)
			return dict.__contains__(self, fkey)
	def __getitem__(self, key):
		fkey = '{}_FILE'.format(key)
		if dict.__contains__(self, fkey):
			with open(dict.__getitem__(self, fkey)) as f:
				return f.read().strip()
		else:
			return dict.__getitem__(self, key)

class AmpelArgumentParser(ArgumentParser):
	def __init__(self, *args, **kwargs):
		super(AmpelArgumentParser, self).__init__(*args, **kwargs)
		self._resources = {}

		action = self.add_argument('-c', '--config', type=partial(load_config, gather_plugins=False),
		    default=DEFAULT_CONFIG,
		    help='Path to Ampel config file in JSON format')
		# parse a first pass to get the resource defaults
		argv = [v for v in sys.argv[1:] if not v in ('-h', '--help')]
		opts, argv = super(AmpelArgumentParser, self).parse_known_args(argv)
		self._resource_defaults = opts.config.get('resources', None)
		action.type = load_config

	def require_resource(self, name, roles=[]):
		if name in self._resources:
			return
		entry = next(pkg_resources.iter_entry_points('ampel.pipeline.resources', name), None)
		if entry is None:
			raise NameError("Resource {} is not defined".format(name))
		resource = entry.resolve()
		resource.add_arguments(self, resource.parse_default(self._resource_defaults), roles)
		self._resources[name] = resource
	
	def require_resources(self, *names):
		"""
		Request configuration of URIs for resources of the form RESOURCE.ROLE.
		"""
		# Build unique set of roles needed for each requested resource
		from collections import defaultdict
		roles = defaultdict(set)
		for name in names:
			if '.' in name:
				name, role = name.split('.')
				roles[name].add(role)
			else:
				roles[name].add('default')
		for name, roleset in roles.items():
			if roleset == {'default'}:
				self.require_resource(name)
			else:
				self.require_resource(name, roleset)

	def parse_known_args(self, *args, **kwargs):
		kwargs['env_vars'] = file_environ()
		args, argv = super(AmpelArgumentParser, self).parse_known_args(*args, **kwargs)
		args.config['resources'] = {}
		for name, klass in self._resources.items():
			args.config['resources'][name] = klass.parse_args(args)
		AmpelConfig.set_config(args.config)
		return args, argv
