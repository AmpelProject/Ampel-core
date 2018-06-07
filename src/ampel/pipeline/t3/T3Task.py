#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Task.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 06.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from functools import reduce


class T3Task:
	"""
	Instances of this class are typically created using 
	static method T3TaskLoader.load(...)
	"""

	def __init__(self, task_doc, T3_class, t3_base_config, t3_run_config, logger=None):
		"""
		t3_unit_doc: dict instance containing t3 unit configuration 
		T3_class: class (not instance) of t3 unit associated with this task
		t3_run_config: dict instance
		"""

		self.task_doc = task_doc
		self._t3_run_config = t3_run_config
		self._t3_base_config = t3_base_config
		self._T3_class = T3_class


	def get_config(self, doc_key):
		"""
		"""
		return reduce(dict.get, doc_key.split("."), self.task_doc)


	def get_t3_run_config(self):
		"""
		returns run_config dict instance
		"""
		return self._t3_run_config


	def new_t3_unit(self, logger):
		"""
		returns an instance of a child class of AbsT3Unit 
		"""
		# Instantiate T3 class 
		return self._T3_class(logger, base_config=self._t3_base_config)
