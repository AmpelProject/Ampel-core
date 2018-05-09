#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/T0Channel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 03.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce
from ampel.pipeline.config.Channel import Channel


class T0Channel(Channel):
	"""
	"""

	def __init__(self, doc_channel, source, filter_func, t2_units):
		"""
		doc_channel: dict instance containing channel configrations
		"""
		# Instanciate ampel.pipeline.config.Channel
		super().__init__(doc_channel, source)

		# Build these two log entries once and for all
		self.log_accepted = " -> Channel '%s': alert passes filter criteria" % self.name
		self.log_rejected = " -> Channel '%s': alert was rejected" % self.name
		if self.get_config('parameters.autoComplete'):
			self.log_auto_complete = " -> Channel '%s': accepting alert (auto-complete)" % self.name

		# Save references to the set of t2 unit names (string) and the filter function (apply)
		self.t2_units = t2_units
		self.filter_func = filter_func


	def get_filter_func(self):
		""" """
		return self.filter_func


	def get_t2_units(self):
		""" """
		return self.t2_units


	def get_t2_run_config(self, t2_unit_name):
		""" """
		for el in self.get_config("t2Compute"):
			if el['t2Unit'] == t2_unit_name:
				return el['runConfig']

		return None 


	def set_filter_parameter(self, param_name, param_value):
		"""	
		Manualy override filter parameters
		"""	
		self.get_source_doc()['t0filter']['runConfig'][param_name] = param_value
