#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

import sys
from functools import partial
import inspect
import json
import pkg_resources
from ampel.pipeline.config.AmpelConfig import AmpelConfig

from ampel.pipeline.t3.T3JobLoader import T3JobLoader

def load_config(path, gather_plugins=True):
	"""Load the JSON configuration file at path, and add plugins registered via pkg_resources"""
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
	for resource in pkg_resources.iter_entry_points('ampel.pipeline.t2'):
		klass = resource.resolve()
		name = resource.name
		if name in config['t2_units']:
			raise KeyError("{} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
		unit = {
			'classFullPath': klass.__module__,
			'version': klass.version,
			'private': klass.private,
			'upperLimits': klass.upperLimits,
		}
		if hasattr(klass, 'author'):
			unit['author'] = klass.author
		desc = inspect.getdoc(klass)
		parts = desc.split('\n')
		unit['title'] = parts[0]
		unit['description'] = parts[1:]
		config['t2_units'][name] = unit
	for resource in pkg_resources.iter_entry_points('ampel.t2_run_configs'):
		for name, channel_config in resource.resolve()().items():
			if name in config['t2_run_config']:
				raise KeyError("T2 run config {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
			config['t2_run_config'][name] = channel_config
	for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.units'):
		klass = resource.resolve()
		name = resource.name
		if name in config['t3_units']:
			raise KeyError("{} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
		unit = {
			'classFullPath': klass.__module__,
		}
		config['t3_units'][name] = unit
	for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.configs'):
		for name, channel_config in resource.resolve()().items():
			if name in config['t3_run_config']:
				raise KeyError("T3 run config {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
			config['t3_run_config'][name] = channel_config
	for resource in pkg_resources.iter_entry_points('ampel.pipeline.t3.jobs'):
		for name, channel_config in resource.resolve()().items():
			if name in config['t3_jobs']:
				raise KeyError("T3 job {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
			T3JobLoader.job_schema(channel_config)
			config['t3_jobs'][name] = channel_config



	return config

from configargparse import ArgumentParser, Namespace, Action
from os.path import basename, dirname, abspath, realpath
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
		    default=abspath(dirname(realpath(__file__)) + '/../../../../config/messy_preliminary_config.json'),
		    help='Path to Ampel config file in JSON format')
		# parse a first pass to get the resource defaults
		argv = [v for v in sys.argv[1:] if not v in ('-h', '--help')]
		opts, argv = super(AmpelArgumentParser, self).parse_known_args(argv)
		self._resource_defaults = opts.config.get('resources', None)
		action.type = load_config

	def require_resource(self, name, roles=None):
		if name in self._resources:
			return
		entry = next(pkg_resources.iter_entry_points('ampel.pipeline.resources', name), None)
		if entry is None:
			raise NameError("Resource {} is not defined".format(name))
		resource = entry.resolve()
		resource.add_arguments(self, self._resource_defaults, roles)
		self._resources[name] = resource
	
	def require_resources(self, *names):
		for name in names:
			self.require_resource(name)

	def parse_args(self, **kwargs):
		return super(AmpelArgumentParser, self).parse_args(**kwargs)

	def parse_known_args(self, **kwargs):
		kwargs['env_vars'] = file_environ()
		args, argv = super(AmpelArgumentParser, self).parse_known_args(**kwargs)
		args.config['resources'] = {}
		for name, klass in self._resources.items():
			args.config['resources'][name] = klass(args)
		AmpelConfig.set_config(args.config)
		return args, argv
