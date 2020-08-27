#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/plot/T2SVGQuery.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.06.2019
# Last Modified Date: 15.06.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

# type: ignore[import]

from ampel.plot.SVGQuery import SVGQuery

class T2SVGQuery(SVGQuery):
	"""
	"""

	def __init__(
		self, col="blend", tran_id=None, plot_tag=None, plot_tags=None,
		t2_class_name=None, t2_run_config=None
	):
		""" """

		super().__init__(
			plot_path='body.output.plots',
			tran_id=tran_id,
			plot_tag=plot_tag,
			plot_tags=plot_tags
		)

		self.col = col

		if t2_class_name:
			self.set_t2_class_name(t2_class_name)

		if t2_run_config:
			self.set_t2_run_config(t2_run_config)


	def set_t2_class_name(self, t2_class_name):
		""" """
		self._query['t2Id'] = t2_class_name


	def set_t2_run_config(self, run_config_name):
		""" """
		self._query['run_config'] = run_config_name
