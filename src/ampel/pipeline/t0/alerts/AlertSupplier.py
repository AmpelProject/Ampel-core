#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.abstract.AbsAlertParser import AbsAlertParser


class AlertSupplier:

 
	def __init__(self, alert_loader, alert_parser):
		"""
		"""

		if not issubclass(alert_loader.__class__, AbsAlertLoader):
			raise ValueError("First argument must be a child class of AbsAlertLoader")

		if not issubclass(alert_parser.__class__, AbsAlertParser):
			raise ValueError("Second argument must be a child class of AbsAlertParser")

		self.alert_loader = alert_loader
		self.alert_parser = alert_parser
 

	def get_alerts(self):
		
		parse_func = self.alert_parser.parse

		for alert_bytes in self.alert_loader.load_alerts():
			yield parse_func(alert_bytes)
