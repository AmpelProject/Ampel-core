#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence
from ampel.model.t0.APChanData import APChanData
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsAlertIngester(metaclass=AmpelABC):
	"""
	"""

	@abstractmethod
	def ingest(self, tran_id, pps_alert, uls_alert, list_of_t2_units):
		""" """

	@abstractmethod
	def set_log_id(self, log_id):
		""" """

	@abstractmethod
	def set_config(self, chan_data: Sequence[APChanData]):
		""" """
