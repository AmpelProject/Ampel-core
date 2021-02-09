#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/plot/SVGLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 03.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

# type: ignore[import]

from ampel.db.AmpelDB import AmpelDB
from ampel.plot.SVGCollection import SVGCollection
from ampel.plot.SVGQuery import SVGQuery
from ampel.plot.T2SVGQuery import T2SVGQuery
from collections import defaultdict

class SVGLoader:

	def __init__(self, data_query=None, t2_query=None):
		self._data_query = data_query
		self._t2_query = t2_query
		self._plots = defaultdict(SVGCollection)


	@staticmethod
	def load_all(
		tran_id=None, plot_tag=None, plot_tags=None,
		t2_class_name=None, t2_run_config=None
	):

		data_query = SVGQuery(
			tran_id = tran_id,
			plot_tag = plot_tag,
			plot_tags = plot_tags
		)

		t2_query = T2SVGQuery(
			tran_id = tran_id,
			plot_tag = plot_tag,
			plot_tags = plot_tags,
			t2_class_name = t2_class_name,
			t2_run_config = t2_run_config
		)

		sl = SVGLoader(
			data_query = data_query,
			t2_query = t2_query
		)

		sl.load_plots()

		return sl


	def load_plots(self):

		if self._data_query is not None:

			for el in AmpelDB.get_collection("t0").find(self._data_query._query):
				self._load_plots(
					el['stockId'],
					self._data_query, el['plots']
				)

		if self._t2_query is not None:

			for el in AmpelDB.get_collection("t2").find(self._t2_query._query):
				if 'body' not in el or not el['body']:
					continue
				if 'output' in el['body'][-1] and 'plots' in el['body'][-1]['output']:
					self._load_plots(
						el['stockId'],
						self._t2_query,
						el['body'][-1]['output']['plots']
					)


	def _load_plots(self, tran_id, query, plots):

		for p in plots:

			if query.tag:
				if query.tag not in p['tag']:
					continue

			if query.tags:
				if not all(x in p['tag'] for x in query.tags):
					continue

			self._plots[tran_id].add_raw_db_dict(p)
