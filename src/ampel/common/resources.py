#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import Resource, ResourceURI, argparse
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from os import environ
from urllib.parse import urlparse

class LiveMongoURI(ResourceURI):
	"""Connection to live transient database"""

	name = "mongo"
	fields = ('hostname', 'port')
	roles = ('writer', 'logger')

	@classmethod
	def get_default(cls):
		return dict(scheme='mongodb', hostname='localhost', port=27017)

class Graphite(ResourceURI):
	"""Graphite metrics collector"""

	name = "graphite"
	fields = ('hostname', 'port', 'path')

	@classmethod
	def get_default(cls):
		return dict(scheme='graphite', hostname='localhost', port=2003)

	def __call__(self):
		parts = urlparse(self.uri)

		config = dict(server=parts.hostname, port=parts.port)
		if parts.path == '/':
			import socket
			hostname = socket.gethostname().split('.')[0]
			config['systemName'] = "ampel.{}".format(hostname)
		else:
			config['systemName'] = parts.path[1:]

		return GraphiteFeeder(config)

class SlackToken(Resource):

	name = "slack"

	class BuildValue(argparse.Action):
		def __call__(self, parser, namespace, values, option_string):
			parts = option_string.strip('-').split('-')
			target = parts.pop(0)
			if len(parts) == 1:
				role = None
			else:
				role = parts.pop(0)
			prop = parts.pop(0)
			assert len(parts) == 0
			target += '_tokens'
			if role is None:
				setattr(namespace, target, {prop: values})
			else:
				getattr(namespace, target)['roles'][role] = {prop: values}

	@classmethod
	def parse_default(cls, resource_section):
		return resource_section.get(cls.name, {'roles': {}})

	@classmethod
	def add_arguments(cls, parser, defaults=None, roles=None):
		group = parser.add_argument_group(cls.name, cls.__doc__)
		default_key = cls.name+"_tokens"
		parser.set_defaults(**{default_key: defaults})
		for role in roles:
			prop = 'token'
			group.add_argument('--{}-{}-{}'.format(cls.name, role, prop),
			    env_var='{}_{}_{}'.format(cls.name.upper(), role.upper(), prop.upper()),
			    action=cls.BuildValue, default=argparse.SUPPRESS)

	@staticmethod
	def render_tokens(value):
		return {k: v['token'] for k, v in value['roles'].items()}

	@classmethod
	def parse_args(cls, args):
		key = cls.name+'_tokens'
		return cls.render_tokens(getattr(args, key))
