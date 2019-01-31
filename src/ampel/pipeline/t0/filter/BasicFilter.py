#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/BasicFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import operator
from types import MappingProxyType

from ampel.base.abstract.AbsAlertFilter import AbsAlertFilter

class BasicFilter(AbsAlertFilter):

	version = 1.0

	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'==': operator.eq,
		'!=': operator.ne
	}


	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):

		self.on_match_default_t2_units = on_match_t2_units

		if run_config is None or type(run_config) not in (dict, MappingProxyType):
			raise ValueError("run_config type must be a dict or MappingProxyType (got {})".format(type(dict)))

		self.param = {
			'operator': BasicFilter.ops[
				run_config['operator']
			],
			'criteria': run_config['criteria'],
			'len': run_config['len']
		}

		logger.info("Following BasicFilter criteria were configured: %s" % self.param)


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
			return self.on_match_default_t2_units

		return None
