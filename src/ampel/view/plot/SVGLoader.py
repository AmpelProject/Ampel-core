#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ZTF/Ampel/src/ampel/view/plot/SVGLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 14.06.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.view.plot.SVGCollection import SVGCollection
from collections import defaultdict

class SVGLoader:
	"""
	"""

	def __init__(
		self, tran_id=None, plot_tag=None, plot_tags=None,
		query_data=True, query_t2=True, 
		t2_unit_id=None, t2_run_config=None
	):
		""" """

		self._data_query = {} if query_data else None
		self._t2_query = {} if query_t2 else None
		self._plots = defaultdict(SVGCollection)
		self.tag = None
		self.tags = None

		if tran_id:
			self.set_tran_id(tran_id)

		if t2_unit_id:
			self.set_t2_unit_id(t2_unit_id)

		if t2_run_config:
			self.set_t2_run_config(t2_run_config)

		if plot_tag:
			self.set_plot_tag(plot_tag)

		if plot_tags:
			self.set_plot_tags(plot_tags)


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


	def set_plot_tag(self, tag):
		""" """
		self.tag = tag
		self._data_query['plots.tags'] = tag
		self._t2_query['results.output.plots.tags'] = tag


	def set_plot_tags(self, tags):
		""" """
		self.tags = tags
		self._data_query['plots.tags'] = {'$all': tags}
		self._t2_query['results.output.plots.tags'] = {'$all': tags}


	def set_t2_query_parameter(self, name, value, overwrite=False):
		""" 
		For example:
		set_t2_query_parameter(
			"$or", [
				{'t2UnitId': 'myFirstT2', 'runConfig': 'default'}, 
				{'t2UnitId': 'myT2'}
			]
		)
		"""
		if name in self._t2_query and not overwrite:
			raise ValueError(
				"Parameter %s already defined (use overwrite=True if you know what you're doing)" % name
			)

		self._t2_query[name] = value


	def set_t2_unit_id(self, t2_unit_id):
		""" """
		self._t2_query['t2UnitId'] = t2_unit_id


	def set_t2_run_config(self, run_config_name):
		""" """
		self._t2_query['runConfig'] = run_config_name


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
