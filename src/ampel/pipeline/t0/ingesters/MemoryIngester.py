#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/MemoryIngester.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.TransientFlags import TransientFlags
from ampel.base.AbstractAlertIngester import AbstractAlertIngester


class MemoryIngester(AbstractAlertIngester):

	"""
		Ingester class called by t0.AlertProcessor.
		This class is intended to be used for developing and testing filters.
		It stores transient candidates into accepted and rejected array 
		based on the configured filter outcomes.
	"""

	def __init__(self, flag_index):
		self.accepted_transients = []
		self.rejected_transients = []
		self.accepted_transient_ids = []
		self.rejected_transient_ids = []
		self.flag_index = flag_index


	def ingest(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):

		if all_channels_t2_flags[self.flag_index] is not None:
			self.accepted_transients.append(alert_pps_list)
			self.accepted_transient_ids.append(tran_id)
		else:
			self.rejected_transients.append(alert_pps_list)
			self.rejected_transient_ids.append(tran_id)


	def set_channel_flag(self, f):
		self.channel_flag = f


	def set_job_id(self, job_id):
		pass 


	def map_channel_to_transient_flag(self, transient_flag_list):
		pass 


	def get_accepted(self):
		return self.accepted_transients;


	def get_rejected(self):
		return self.rejected_transients;


	def get_accepted_ids(self):
		return self.accepted_transient_ids;


	def get_rejected_ids(self):
		return self.rejected_transient_ids;

