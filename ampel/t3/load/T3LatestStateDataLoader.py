#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/load/T3LatestStateDataLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from typing import Iterable, Union, Iterator
from ampel.type import StockId, StrictIterable
from ampel.t3.load.AbsT3Loader import AbsT3Loader
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.util.collections import to_set
from ampel.config.LogicSchemaUtils import LogicSchemaUtils
from ampel.db.query.latest_compound import fast_query, general_query


class T3LatestStateDataLoader(AbsT3Loader):
	"""
	Load only T1 and T2 documents associated with the latest state of each
	stock in the selected channels. The latest state is the compound doc with
	the largest value of ``len`` or ``added``.

	.. note::
	  
	  T3LatestStateDataLoader can only load state-bound T2 documents. If you
	  need point- or stock-bound T2s, use
	  :py:class:`~ampel.t3.load.T3SimpleDataLoader.T3SimpleDataLoader`.

	.. seealso::
	  
	  :py:func:`ampel.db.query.latest_compound.fast_query`
	    for notes on how compounds are selected from T0
	  
	  :py:func:`ampel.db.query.latest_compound.general_query`
	    for notes on how compounds are selected from other tiers
	"""


	def __init__(self, context: AmpelContext, **kwargs):

		super().__init__(context, **kwargs)

		self.col_t1 = context.db.get_collection("t1")

		for directive in self.directives:

			if directive.col == "t1":
				if directive.query_complement and '_id' in directive.query_complement:
					raise ValueError("query complement parameter '_id' cannot be used on t1 collection")

			if directive.col == "t2":
				if directive.query_complement and 'link' in directive.query_complement:
					raise ValueError("query complement parameter 'link' cannot be used on t2 collection")


	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]]
	) -> Iterable[AmpelBuffer]:
		"""
		Warning: if stock_ids is an Iterator, it will be fully consumed
		"""

		if isinstance(stock_ids, collections.abc.Iterator):
			stock_ids = list(stock_ids)

		# self.logger.info(f"Loading {len(stock_ids)} transients")
		states = None

		# determine latest compId of each transient
		self.logger.info("Determining latest state")

		# ids for which the fast query cannot be used (results cast into set)
		slow_ids = set(
			el['stock'] for el in self.col_t1.find(
				{
					'stock': {'$in': stock_ids},
					'tier': {'$ne': 0}
				},
				{'_id': 0, 'stock': 1}
			)
		)

		# set of transient states (see comment below for an example)
		states = set()
		set_stock_ids = to_set(stock_ids)

		# Channel/Channels must be provided if state is 'latest'
		# Get latest state ** for each channel(s) criteria **
		for chan_logic in LogicSchemaUtils.iter(self.channel):

			# get latest state (fast mode)
			# Output example:
			# [
			# {
			#   '_id': b']\xe2H\x0f(\xbf\xca\x0b\xd3\xba\xae\x89\x0c\xb2\xd2\xae',
			#   'stock': 1810101034343026   # (ZTF18aaayyuq)
			# },
			# {
			#   '_id': b'_\xcd\xed\xa5\xe1\x16\x98\x9ai\xf6\xcb\xbd\xe7#FT',
			#   'stock': 1810101011182029   # (ZTF18aaabikt)
			# },
			# ...
			# ]
			states.update(
				[
					el['_id'] for el in self.col_t1.aggregate(
						fast_query(
							list(slow_ids.symmetric_difference(set_stock_ids)),
							channel = chan_logic
						)
					)
				]
			)

			# TODO: check result length ?

			# get latest state (general mode) for the remaining transients
			for slow_id in slow_ids:

				# get latest state for single transients using general query
				latest_state = next(
					self.col_t1.aggregate(
						general_query(slow_id, project={'$project': {'_id': 1}})
					),
					None
				)

				# Robustness
				if latest_state is None:
					# TODO: add error flag to transient doc ?
					# TODO: add error flag to event doc
					# TODO: add doc to Ampel_troubles
					self.logger.error(
						f"Could not retrieve latest state for transient {slow_id}"
					)
					continue

				states.add(
					latest_state['_id']
				)


		# Customize T1 & T2 queries (add state query parameter)
		directives = []

		for directive in self.directives:

			if directive.col in ("t1", "t2"):

				qd = directive.copy(deep=True)
				key = 'link' if directive.col == 't2' else '_id'

				if qd.query_complement:
					qd.query_complement[key] = {"$in": list(states)}
				else:
					qd.query_complement = {key: {"$in": list(states)}}

				directives.append(qd)

			else:
				directives.append(directive)

		self.logger.info("Loading ampel data")

		# Returns AmpelBuffer instances
		return self.db_content_loader.load(
			stock_ids = stock_ids,
			directives = directives,
			channel = self.channel
		)
