#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/BasicFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2018
# Last Modified Date: 11.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter
import operator

class BasicFilter(AbstractTransientFilter):

	version = 0.1

	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'=': operator.eq
	}


	def get_version(self):
		return BasicFilter.version


	def set_filter_parameters(self, filter_parameters):
		"""
		Doc will follow
		"""

		if type(filter_parameters) is not dict:
			raise ValueError("Method parameter must be a dict instance")

		self.param = {
			'operator': BasicFilter.ops[
				filter_parameters['operator']
			],
			'criteria': filter_parameters['criteria'],
			'len': filter_parameters['len']
		}

		self.logger.info("Following filter parameter was set: %s" % self.param)


	def apply(self, ampel_alert):
		"""
		Doc will follow
		"""

		if self.param['operator'](
			len(
				ampel_alert.get_values(
					'candid', 
					filters = self.param['criteria']
				)
			),
			self.param['len']
		):
			return self.on_match_default_flags

		return None
