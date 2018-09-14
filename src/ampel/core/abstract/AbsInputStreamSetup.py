#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelDataSource.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 06.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsInputStreamSetup(AmpelABC):
	"""
	Depending on which instrument and institution alerts originate,
	(as of March 2018 only ZTF & IPAC), various setup tasks must be 
	performed before alert ingestion happens (ex: static settings may be set in AmpelAlert).
	Please perform those tasks in __init__().

	Also, the following two methods must be implemented:
	-> get_alert_supplier(...): iterable class instance that for each alert yielded by 
	the alert_loader, returns a dict with a format that the AMPEL AlertProcessor understands
	-> get_alert_ingester(...): returns an adequate ingester instance
	"""

	@abstractmethod
	def get_alert_supplier(self, alert_loader):
		pass

	@abstractmethod
	def get_alert_ingester(self, channels, logger):
		pass
