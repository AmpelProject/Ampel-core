#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/plot/SVGLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 15.06.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.db.AmpelDB import AmpelDB
from ampel.view.plot.SVGCollection import SVGCollection
from ampel.view.plot.SVGQuery import SVGQuery
from ampel.view.plot.T2SVGQuery import T2SVGQuery
from collections import defaultdict

class SVGLoader:
	"""
	"""

	def __init__(self, data_query=None, t2_query=None):
		""" """

		self._data_query = data_query
		self._t2_query = t2_query
		self._plots = defaultdict(SVGCollection)


	@staticmethod	
	def load_all(
		tran_id=None, plot_tag=None, plot_tags=None,
		t2_unit_id=None, t2_run_config=None
	):
		""" """

		data_query = SVGQuery(
			tran_id=tran_id, 
			plot_tag=plot_tag, 
			plot_tags=plot_tags
		)

		t2_query = T2SVGQuery(
			tran_id=tran_id, 
			plot_tag=plot_tag, 
			plot_tags=plot_tags,
			t2_unit_id=t2_unit_id, 
			t2_run_config=t2_run_config
		)

		sl = SVGLoader(
			data_query=data_query, 
			t2_query=t2_query
		)

		sl.load_plots()

		return sl


	def load_plots(self):
		""" """

		if self._data_query is not None:

			for el in AmpelDB.get_collection("photo").find(self._data_query._query):
				self._load_plots(
					el['tranId'], 
					self._data_query, el['plots']
				)

		if self._t2_query is not None:

			for el in AmpelDB.get_collection("blend").find(self._t2_query._query):
				if 'results' not in el or not el['results']:
					continue
				if 'output' in el['results'][-1] and 'plots' in el['results'][-1]['output']:
					self._load_plots(
						el['tranId'], 
						self._t2_query, 
						el['results'][-1]['output']['plots']
					)


	def _load_plots(self, tran_id, query, plots):
		""" """

		for p in plots:
			
			if query.tag:
				if not query.tag in p['tags']:
					continue

			if query.tags:
				if not all(x in p['tags'] for x in query.tags):
					continue
					
			self._plots[tran_id].add_raw_db_dict(p)
