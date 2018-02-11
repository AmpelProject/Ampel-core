#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbstractAlertIngester.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 27.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbstractAlertIngester(metaclass=AmpelABC):


	@abstractmethod
	def set_job_id(self, job_id):
		return


	@abstractmethod
	def ingest(self, tran_id, pps_alert, list_of_t2_runnables):
		return
