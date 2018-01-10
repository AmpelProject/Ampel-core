#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/TestIngester.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AbstractAlertIngester import AbstractAlertIngester


class TestIngester(AbstractAlertIngester):

	def __init__(self):
		print("init")

	def set_job_id(self, job_id):
		pass
	
	def ingest(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):
		print("apply")
