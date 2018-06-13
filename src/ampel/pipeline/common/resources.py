#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import ResourceURI
from ampel.pipeline.common.expandvars import expandvars
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from os import environ
from urllib.parse import urlparse

class LiveMongoURI(ResourceURI):
	"""Connection to live transient database"""

	name = "mongo"
	fields = ('hostname', 'port', 'username', 'password')

	@classmethod
	def get_default(cls):
		return dict(scheme='mongodb', hostname='localhost', port=27017)

	def __call__(self):
		return self.uri

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
