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
		parts = urlparse(uri)
		if parts.path == '/':
			hostname = socket.gethostname().split('.')[0]
			system_name = "ampel.{}".format(hostname)
		else:
			system_name = parts.path[1:]
		self.gclient = GraphiteClient(
			graphite_server=getattr(parts, 'hostname'), 
			graphite_port=getattr(parts, 'port'), 
			system_name=system_name,
			autoreconnect=True
		)

		self.stats = {}


	def add_stat(self, key, value, suffix=""):
		"""
		"""
		if len(suffix) > 0 and suffix[-1] != ".": suffix += "."
		self.stats[suffix + key] = value


	def add_stats(self, in_dict, suffix=""):
		"""
		"""
		if len(suffix) > 0 and suffix[-1] != ".": suffix += "."

		fdict = AmpelUtils.flatten_dict(in_dict)
		for key in fdict:
			self.stats[suffix + key] = fdict[key]


	def add_stats_with_mean_std(self, in_dict, suffix=""):
		"""
		"""
		if len(suffix) > 0 and suffix[-1] != ".": suffix += "."

		fdict = AmpelUtils.flatten_dict(in_dict)
		key_to_delete = []

		for key in fdict:
			val = fdict[key]
			if type(val) in [tuple, list]:
				if len(val) == 2: 
					self.stats[suffix + key + ".mean"] = val[0]
					self.stats[suffix + key + ".std"] = val[1]
				elif len(val) == 0: # empty list
					key_to_delete.append(key)
			else:
				self.stats[suffix + key] = val

		for key in key_to_delete:
			del fdict[key] # remove empty lists
			

	def send(self):
		"""
		"""
		self.gclient.send_dict(self.stats)
		self.stats = {}
