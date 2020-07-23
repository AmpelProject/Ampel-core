#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/resource/Graphite.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.config.resource.resources import ResourceURI
from ampel.metrics.GraphiteFeeder import GraphiteFeeder
from urllib.parse import urlparse

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
