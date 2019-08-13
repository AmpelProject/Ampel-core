#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/GraphiteFeeder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.04.2018
# Last Modified Date: 12.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import socket
from urllib.parse import urlparse
from graphitesend import GraphiteClient
from ampel.pipeline.common.AmpelUtils import AmpelUtils

class GraphiteFeeder:
	"""
	"""

	def __init__(self, uri, autoreconnect=True):
		"""
		"""

		# URI example: graphite://localhost:2003/
		parts = urlparse(uri)

		# Generate sytem name if not provided in URI
		if parts.path == '/':
			hostname = socket.gethostname().split('.')[0]
			system_name = "ampel.{}".format(hostname)
		else:
			system_name = parts.path[1:]

		# Instanciate GraphiteClient
		self.gclient = GraphiteClient(
			graphite_server=getattr(parts, 'hostname'), 
			graphite_port=getattr(parts, 'port'), 
			system_name=system_name,
			autoreconnect=autoreconnect
		)

		self.stats = {}


	def add_stat(self, key, value, prefix=""):
		"""
		"""
		if len(prefix) > 0 and prefix[-1] != ".": prefix += "."
		self.stats[prefix + key] = value


	def add_stats(self, in_dict, prefix=""):
		"""
		"""
		if len(prefix) > 0 and prefix[-1] != ".": prefix += "."

		fdict = AmpelUtils.flatten_dict(in_dict)
		for key in fdict:
			self.stats[prefix + key] = fdict[key]


	def add_stats_with_mean_std(self, in_dict, prefix=""):
		"""
		"""
		if len(prefix) > 0 and prefix[-1] != ".": prefix += "."

		fdict = AmpelUtils.flatten_dict(in_dict)
		key_to_delete = []

		for key in fdict:
			val = fdict[key]
			if type(val) in [tuple, list]:
				if len(val) == 2: 
					self.stats[prefix + key + ".mean"] = val[0]
					self.stats[prefix + key + ".std"] = val[1]
				elif len(val) == 0: # empty list
					key_to_delete.append(key)
			else:
				self.stats[prefix + key] = val

		for key in key_to_delete:
			del fdict[key] # remove empty lists
			

	def send(self):
		"""
		"""
		self.gclient.send_dict(self.stats)
		self.stats = {}
