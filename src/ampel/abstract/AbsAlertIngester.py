#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsAlertIngester(metaclass=AmpelABC):


	@abstractmethod
	def set_job_id(self, job_id):
		return

	@abstractmethod
	def configure(self, channels):
		return

	@abstractmethod
	def ingest(self, tran_id, pps_alert, list_of_t2_runnables):
		return

	# pylint: disable=no-member
	def get_version(self):
		return self.version
