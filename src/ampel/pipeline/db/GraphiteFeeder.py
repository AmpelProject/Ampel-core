#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/GraphiteFeeder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.04.2018
# Last Modified Date: 21.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from graphitesend import GraphiteClient

class GraphiteFeeder:
	"""
	"""

	def __init__(self, graphite_config):
		"""
		"""
		self.gclient = GraphiteClient(
			graphite_server=graphite_config['server'], 
			graphite_port=graphite_config['port'], 
			system_name=graphite_config['systemName']
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

		fdict = self.flatten_dict(in_dict)
		for key in fdict:
			self.stats[suffix + key] = fdict[key]


	def add_stats_with_mean_std(self, in_dict, suffix=""):
		"""
		"""
		if len(suffix) > 0 and suffix[-1] != ".": suffix += "."

		fdict = self.flatten_dict(in_dict)
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
		print(self.stats)
		self.gclient.send_dict(self.stats)
		self.stats = {}


	def flatten_dict(self, d):
		"""
		"""
		expand = lambda key, val: (
			[(key + '.' + k, v) for k, v in self.flatten_dict(val).items()] 
			if isinstance(val, dict) else [(key, val)]
		)

		items = [item for k, v in d.items() for item in expand(k, v)]

		return dict(items)
