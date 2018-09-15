#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/abstract/AbsAlertIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 15.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsAlertIngester(metaclass=AmpelABC):
	"""
	"""

	@abstractmethod
	def ingest(self, tran_id, pps_alert, uls_alert, list_of_t2_units):
		""" """
		pass

	@abstractmethod
	def set_log_id(self, log_id):
		""" """
		pass
