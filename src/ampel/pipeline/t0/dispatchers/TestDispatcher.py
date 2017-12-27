#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/dispatchers/TestDispatcher.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 27.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.t0.dispatchers.AbstractAmpelDispatcher import AbstractAmpelDispatcher


class TestDispatcher(AbstractAmpelDispatcher):

	def __init__(self):
		print("init")

	def set_job_id(self, job_id):
		pass
	
	#def dispatch(self, transient):
	def dispatch(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):
		print("apply")

