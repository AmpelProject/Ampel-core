#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/TestIngester.py
# Licence           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.03.2018
# Last Modified Date: 02.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsAlertIngester import AbsAlertIngester


class TestIngester(AbsAlertIngester):

	version = 1.0

	def __init__(self):
		print("init")

	def set_job_id(self, job_id):
		pass
	
	def ingest(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):
		print("apply")

	# pylint: disable=no-member
	def get_version(self):
		return self.version
