#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 30.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.abstract.AbsAlertParser import AbsAlertParser
from ampel.abstract.AbsAlertDeserializer import AbsAlertDeserializer


class AlertSupplier:

 
	def __init__(self, alert_loader, alert_parser, deserializer=None):
		"""
		alert_loader: laads and returns alerts (can be bytes or a dict instance)
		deserializer (optional): deserialize bytes into dict 
		alert_parser: reshapes dict into a form compatible with ampel
		"""

		if not issubclass(alert_loader.__class__, AbsAlertLoader):
			raise ValueError("First argument must be a child class of AbsAlertLoader")

		if not issubclass(alert_parser.__class__, AbsAlertParser):
			raise ValueError("Second argument must be a child class of AbsAlertParser")

		if not deserializer is None:
			self.set_deserializer(deserializer)
		else:
			self.deserializer = None

		self.alert_loader = alert_loader
		self.alert_parser = alert_parser
 

	def set_deserializer(self, deserializer):

		if not issubclass(deserializer.__class__, AbsAlertDeserializer):
			raise ValueError("deserializer must be a child class of AbsAlertDeserializer")

		self.deserializer = deserializer


	def get_alerts(self):
		
		shape = self.alert_parser.shape

		if self.deserializer is None:
			while True:
				alert = self.alert_loader.get_next()
				if alert is not None:
					yield shape(alert)
				else:
					return
		else:
			deserialize = self.deserializer.get_dict
			while True:
				alert = self.alert_loader.get_next()
				if alert is not None:
					yield shape(
						deserialize(
							alert
						)
					)
				else:
					return
