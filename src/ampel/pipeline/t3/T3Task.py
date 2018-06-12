#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Task.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 11.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce

class T3Task:
	"""
	Instances of this class are typically created using 
	static method T3TaskLoader.load(...)
	"""

	def __init__(self, task_doc, T3_unit_class, t3_unit_base_config, t3_unit_run_config):
		"""
		t3_unit_doc: dict instance containing t3 unit configuration 
		T3_unit_class: class (not instance) of t3 unit associated with this task
		t3_run_config: dict instance
		"""

		self.task_doc = task_doc
		self._T3_unit_class = T3_unit_class
		self._t3_instance = None
		self._t3_unit_base_config = t3_unit_base_config
		self._t3_unit_run_config = t3_unit_run_config


	def get_config(self, doc_key):
		"""
		doc_key: dict key
		"""
		return reduce(dict.get, doc_key.split("."), self.task_doc)


	def get_t3_unit_run_config(self):
		"""
		returns run_config dict instance
		"""
		return self._t3_unit_run_config


	def get_t3_unit_instance(self, logger):
		"""
		returns an instance of a child class of AbsT3Unit 
		"""
		if self._t3_instance is None:
			# Instantiate T3 class
			self._t3_instance = self._T3_unit_class(
				logger, base_config=self._t3_unit_base_config
			)

		return self._t3_instance


	def run(self, tran_data, logger):
		"""
		"""
		self.get_t3_unit_instance(logger).run(
			self._t3_unit_run_config,  tran_data
		)
