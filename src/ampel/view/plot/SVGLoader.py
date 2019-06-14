#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ZTF/Ampel/src/ampel/view/plot/SVGLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 13.06.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.view.plot.SVGCollection import SVGCollection
from collections import defaultdict

class SVGLoader:
	"""
	"""

	def __init__(
		self, tran_id=None, tag=None, tags=None,
		query_data=True, query_t2=True,
	):
		""" """

		self._data_query = {} if query_data else None
		self._t2_query = {} if query_data else None
		self._plots = defaultdict(SVGCollection)
		self.tag = None
		self.tags = None

		if tran_id:
			self.set_tran_id(tran_id)

		if tag:
			self.set_tag(tag)

		if tags:
			self.set_tags(tags)


	def set_tran_id(self, tran_id):
		""" """

		if self._data_query is not None:
			if isinstance(tran_id, (list, tuple)):
				self._data_query['tranId'] = {'$in' :tran_id}
			else:
				self._data_query['tranId'] = tran_id

		if self._t2_query is not None:
			if isinstance(tran_id, (list, tuple)):
				self._t2_query['tranId'] = {'$in' :tran_id}
			else:
				self._t2_query['tranId'] = tran_id


	def set_tag(self, tag):
		""" """
		self.tag = tag
		self._data_query['plots.tags'] = tag
		self._t2_query['results.output.plots.tags'] = tag


	def set_tags(self, tags):
		""" """
		self.tags = tags
		self._data_query['plots.tags'] = {'$all': tags}
		self._t2_query['results.output.plots.tags'] = {'$all': tags}


	def load_plots(self):
		""" """

		if self._data_query is not None:
			for el in AmpelDB.get_collection("photo").find(self._data_query):
				self._load_plots(el['tranId'], el['plots'])

		if self._t2_query is not None:
			for el in AmpelDB.get_collection("blend").find(self._t2_query):
				if 'output' in el['results'][-1] and 'plots' in el['results'][-1]['output']:
					self._load_plots(el['tranId'], el['results'][-1]['output']['plots'])


	def _load_plots(self, tran_id, plots):
		""" """

		for p in plots:
			
			if self.tag:
				if not self.tag in p['tags']:
					continue

			if self.tags:
				if not all(x in p['tags'] for x in self.tags):
					continue
					
			self._plots[tran_id].add_raw_db_dict(p)
