#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 14.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.abstract.AbsAlertShaper import AbsAlertShaper


class AlertSupplier:

 
	def __init__(self, alert_loader, alert_shaper, serialization=None):
		"""
		alert_loader: laads and returns alerts (can be bytes or a dict instance)
		alert_shaper: reshapes dict into a form compatible with ampel
		serialization (optional): either 'avro' or 'json'. Set the corresponding 
		deserialization function used to convert file_like objects into dict
		"""

		if not issubclass(alert_loader.__class__, AbsAlertLoader):
			raise ValueError("First argument must be a child class of AbsAlertLoader")

		if not issubclass(alert_shaper.__class__, AbsAlertShaper):
			raise ValueError("Second argument must be a child class of AbsAlertShaper")

		if serialization is not None:

			if serialization == "json":
				import json
				self.deserialize = json.load

			elif serialization == "avro":
				import fastavro
				self.deserialize = lambda x: next(fastavro.reader(x), None)

			else:
				raise NotImplementedError(
					"Deserialization of %s not implemented" % serialization
				)

		self.alert_loader = alert_loader
		self.alert_shaper = alert_shaper
 

	def set_deserializer_func(self, deserializer_func):
		"""
		deserializer_func: function deserializing file_like objects into dict 
		"""
		self.deserialize = deserializer_func


	def get_alerts(self):
		"""
		"""
		if self.deserialize is None:
			for file_like in self.alert_loader.get_files():
				yield self.alert_shaper.shape(
					file_like
				)
		else:
			for file_like in self.alert_loader.get_files():
				yield self.alert_shaper.shape(
					self.deserialize(
						file_like
					)
				)
