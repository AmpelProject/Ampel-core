#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/plot/SVGQuery.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.06.2019
# Last Modified Date: 03.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class SVGQuery:

	def __init__(
		self, col="photo", plot_path='plots',
		stock_id=None, plot_tag=None, plot_tags=None
	):

		self._query = {}
		self.tag = None
		self.tags = None
		self.plot_path = plot_path
		self.col = col

		if stock_id:
			self.set_stock_id(stock_id)

		if plot_tag:
			self.set_plot_tag(plot_tag)

		if plot_tags:
			self.set_plot_tags(plot_tags)


	def get_query(self):
		return self._query


	def set_stock_id(self, stock_id):

		if isinstance(stock_id, (list, tuple)):
			self._query['stockId'] = {'$in': stock_id}
		else:
			self._query['stockId'] = stock_id


	def set_plot_tag(self, tag):
		self.tag = tag
		self._query[self.plot_path + ".tags"] = tag


	def set_plot_tags(self, tags):
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
