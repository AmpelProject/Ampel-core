#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ArgumentParser.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import os, sys, pkg_resources, warnings
from functools import partial
from configargparse import ArgumentParser, ArgumentDefaultsRawHelpFormatter
from ampel.pipeline.config.ConfigLoader import ConfigLoader
from ampel.pipeline.config.AmpelConfig import AmpelConfig

class AmpelArgumentParser(ArgumentParser):
	""" """

	def __init__(self, *args, **kwargs):
		""" """
		if not 'formatter_class' in kwargs:
			kwargs['formatter_class'] = ArgumentDefaultsRawHelpFormatter
		super(AmpelArgumentParser, self).__init__(*args, **kwargs)
		self._resources = {}

		action = self.add_argument(
			'-c', '--config', 
			type=partial(ConfigLoader.load_config, gather_plugins=False),
		    default=ConfigLoader.DEFAULT_CONFIG, 
			help='Path to Ampel config file in JSON format',
			env_var='AMPEL_CONFIG',
		)

		# parse a first pass to get the resource defaults
		argv = [v for v in sys.argv[1:] if not v in ('-h', '--help')]
		opts, argv = super(AmpelArgumentParser, self).parse_known_args(argv)
		self._resource_defaults = opts.config.get('resources', None)
		action.type = ConfigLoader.load_config

	def require_resource(self, name, roles=[]):
		""" 
		"""

		if name in self._resources:
			return

		entry = next(
			pkg_resources.iter_entry_points('ampel.pipeline.resources', name), 
			None
		)

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
		""" """
		kwargs['env_vars'] = file_environ()
		args, argv = super(AmpelArgumentParser, self).parse_known_args(*args, **kwargs)
		args.config['resources'] = {}
		for name, klass in self._resources.items():
			args.config['resources'][name] = klass.parse_args(args)
		with warnings.catch_warnings():
			warnings.filterwarnings('ignore', message='resetting global configuration')
			AmpelConfig.set_config(args.config)
		return args, argv


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

