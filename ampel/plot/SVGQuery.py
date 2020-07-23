#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/plot/SVGQuery.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.06.2019
# Last Modified Date: 15.06.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class SVGQuery:
	"""
	"""

	def __init__(
		self, col="photo", plot_path='plots', 
		tran_id=None, plot_tag=None, plot_tags=None
	):
		""" """

		self._query = {}
		self.tag = None
		self.tags = None
		self.plot_path = plot_path
		self.col = col

		if tran_id:
			self.set_tran_id(tran_id)

		if plot_tag:
			self.set_plot_tag(plot_tag)

		if plot_tags:
			self.set_plot_tags(plot_tags)


	def get_query(self):
		""" """
		return self._query


	def set_tran_id(self, tran_id):
		""" """

		if isinstance(tran_id, (list, tuple)):
			self._query['tranId'] = {'$in' :tran_id}
		else:
			self._query['tranId'] = tran_id


	def set_plot_tag(self, tag):
		""" """
		self.tag = tag
		self._query[self.plot_path + ".tags"] = tag


	def set_plot_tags(self, tags):
		""" """
		self.tags = tags
		self._query[self.plot_path + ".tags"] = {'$all': tags}


	def set_query_parameter(self, name, value, overwrite=False):
		""" 
		For example:
		set_query_parameter(
			"$or", [
				{'t2Id': 'myFirstT2', 'run_config': 'default'}, 
				{'t2Id': 'myT2'}
			]
		)
		"""
		if name in self._query and not overwrite:
			raise ValueError(
				"Parameter %s already defined (use overwrite=True if you know what you're doing)" % name
			)

		self._query[name] = value
