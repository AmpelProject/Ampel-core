#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/BasicMultiFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2017
# Last Modified Date: 11.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter
import operator

class BasicMultiFilter(AbstractTransientFilter):

	version = 0.1

	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'=': operator.eq,
		'AND': operator.and_,
		'OR': operator.or_,
	}


	def get_version(self):
		return BasicMultiFilter.version


	def set_filter_parameters(self, parameters):
		"""
		Doc will follow
		"""

		self.filters = []
		self.logical_ops = [None]

		if "logicalConnection" in parameters['filters'][0]:
			raise ValueError("First filter element cannot contain parameter logicalConnection")

		for param in parameters['filters']:

			self.filters.append(
				{
					'operator': BasicMultiFilter.ops[
						param['operator']
					],
					'criteria': param['criteria'],
					'len': param['len']
				}
			)

			if "logicalConnection" in param:
				self.logical_ops.append(
					BasicMultiFilter.ops[
						param["logicalConnection"]
					]
				)


	def apply(self, ampel_alert):
		"""
		Doc will follow
		"""

		filter_res = []

		for param in self.filters:

			filter_res.append(
				param['operator'](
					len(
						ampel_alert.get_values(
							'candid', 
							filters = param['criteria']
						)
					),
					param['len']
				)
			)

		current_res = False

		for i, param in enumerate(filter_res):

			if i == 0:
				current_res = filter_res[i]
			else: 
				current_res = self.logical_ops[i](
					current_res, filter_res[i]
				)

		if current_res:
			return self.on_match_default_flags

		return None
