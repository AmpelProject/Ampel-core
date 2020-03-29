#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/metrics/GraphiteFeeder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.04.2018
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import socket
from typing import Dict, Any
from urllib.parse import urlparse
from graphitesend import GraphiteClient
from ampel.utils.mappings import flatten_dict


class GraphiteFeeder:


	def __init__(self, uri: str, autoreconnect: bool = True) -> None:

		# URI example: graphite://localhost:2003/
		parts = urlparse(uri)

		# Generate sytem name if not provided in URI
		if parts.path == '/':
			hostname = socket.gethostname().split('.')[0]
			system_name = f"ampel.{hostname}"
		else:
			system_name = parts.path[1:]

		# Instanciate GraphiteClient
		self.gclient = GraphiteClient(
			graphite_server=getattr(parts, 'hostname'),
			graphite_port=getattr(parts, 'port'),
			system_name=system_name,
			autoreconnect=autoreconnect
		)

		self.stats: Dict[str, Any] = {}


	def add_stat(self, key: str, value: Any, prefix: str = "") -> None:
		"""
		"""
		if len(prefix) > 0 and prefix[-1] != ".":
			prefix += "."
		self.stats[prefix + key] = value


	def add_stats(self, in_dict: Dict, prefix: str = "") -> None:
		"""
		"""
		if len(prefix) > 0 and prefix[-1] != ".":
			prefix += "."

		fdict = flatten_dict(in_dict)

		for key in fdict:
			self.stats[prefix + key] = fdict[key]


	def add_stats_with_mean_std(self, in_dict, prefix="") -> None:
		"""
		"""
		if len(prefix) > 0 and prefix[-1] != ".":
			prefix += "."

		fdict = flatten_dict(in_dict)
		key_to_delete = []

		for key in fdict:
			val = fdict[key]
			if isinstance(val, (tuple, list)):
				if len(val) == 2:
					self.stats[prefix + key + ".mean"] = val[0]
					self.stats[prefix + key + ".std"] = val[1]
				elif len(val) == 0: # empty list
					key_to_delete.append(key)
			else:
				self.stats[prefix + key] = val

		for key in key_to_delete:
			del fdict[key] # remove empty lists


	def send(self) -> None:
		"""
		"""
		self.gclient.send_dict(self.stats)
		self.stats = {}
