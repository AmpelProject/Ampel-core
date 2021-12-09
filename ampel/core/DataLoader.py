#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/DataLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 02.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.codec_options import CodecOptions
from typing import Iterable, Union, Dict, Iterator, Optional, Literal

from ampel.types import StockId, ChannelId, StrictIterable, Tag
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.t3.LoaderDirective import LoaderDirective
from ampel.mongo.view.FrozenValuesDict import FrozenValuesDict
from ampel.mongo.query.general import build_general_query
from ampel.log.utils import safe_query_dict
from ampel.log.AmpelLogger import AmpelLogger
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.util.collections import ampel_iter
from ampel.core.AmpelContext import AmpelContext
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry

# Monitoring counters
stat_db_loads = AmpelMetricsRegistry.counter(
	"loads", "Number of documents loaded",
	subsystem="db", labelnames=("col",)
)

class DataLoader:

	def __init__(self, ctx: AmpelContext) -> None:
		self.ctx = ctx

	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]],
		directives: Iterable[LoaderDirective],
		channel: Union[None, ChannelId, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]] = None,
		tag: Optional[Dict[Literal['with', 'without'], Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]]] = None,
		auto_project: bool = True,
		codec_options: CodecOptions = CodecOptions(document_class=FrozenValuesDict),
		logger: Optional[AmpelLogger] = None
	) -> Iterable[AmpelBuffer]:
		"""
		:param directives: see LoaderDirective docstrings for more information.  Notes:
		- T3 unit configurations can use aliased directives. For example, the alias COMPOUND
		is translated into the directive LoaderDirective(col="t1")
		- LoaderDirective can contain "query_complement", which can be used among other thing to further
		sub-select t2 documents or select given states (such as the latest state)

		:param tag: If specified, query selection critera will apply to all directives.

		:param channel: If specified, query selection critera will apply to all directives (except t0).
		None means all channels are considered (no criterium).

		:param auto_project: each LoaderDirective is associated with a db collection with is itself associated with
		a default "content" typed dict. By default, we request a projection that projects only the fields defined
		in those TypedDict. Custom/admin fields would thus not be retrieved.
		Set this setting to False if it is not the whished behavior.
		"""

		col_set = {directive.col for directive in directives}

		# Note: the following operation will consume
		# stock_ids if it is an Iterator/Cursor
		register: Dict[StockId, AmpelBuffer] = {
			stock_id: AmpelBuffer(
				id = stock_id,
				stock = None if "stock" in col_set else None,
				t0 = [] if "t0" in col_set else None,
				t1 = [] if "t1" in col_set else None,
				t2 = [] if "t2" in col_set else None
			) for stock_id in ampel_iter(stock_ids)
		}

		for directive in directives:

			query = build_general_query(
				stock=register.keys(), channel=channel, tag=tag
			)

			#if directive.col == "stock":
			#	query['_id'] = query.pop("stock")
			#elif directive.col == "t0" and channel:
			#	query.pop('channel')

			# query 'stock' parameter primes over query complements
			if directive.query_complement:
				query = directive.query_complement | query

			if logger and logger.verbose > 1: # log query parameters
				logger.debug(
					None, extra={
						'col': directive.col,
						'query': safe_query_dict(query, dict_key=None)
					}
				)

			# Retrieve pymongo cursor
			col = self.ctx.db.get_collection(directive.col)

			if codec_options:
				col = col.database.get_collection(col.name, codec_options=codec_options)

			# Note: codec_options freezes structures in dicts with depth level > 1
			cursor = col.find(
				filter = query,
				projection = {
					k: 1 for k in directive.model.__annotations__.keys()
				} if auto_project else None
			)

			inc = stat_db_loads.labels(directive.col).inc

			if directive.col == "t1":
				count = 0
				for count, res in enumerate(cursor, 1):
					register[res['stock']][directive.col].append(res) # type: ignore[union-attr]
				inc(count)

			elif directive.col == "stock":
				count = 0
				for count, res in enumerate(cursor, 1):
					register[res['stock']]['stock'] = res
				inc(count)

			# Datapoints are potentially channel-less and can be associated with multiple stocks
			elif directive.col == "t0":

				count = 0
				for count, res in enumerate(cursor, 1):

					# Upper limits can be attached to multiple transients
					for sid in res['stock']:

						# Some of which might not match with our query
						if sid not in register:
							continue

						# Add datapoints to snapdata (no need to recursive_freeze
						# since dict elements with level > 1 are frozen due to codec_options
						register[sid]['t0'].append(res) # type: ignore[union-attr]
				inc(count)

			elif directive.col == "t2":

				# the entire data will need to fit in memory anyway
				res = list(cursor)

				# whether to replace init config integer hash with 'resolved' config dict
				if directive.resolve_config:

					config_keys = self.ctx.config.get('t2.config_keys', dict)
					if not config_keys:
						raise ValueError("Cannot load t2 configs")

					for el in res:
						#: init config integer hash with 'resolved' config dict
						dict.__setitem__(el, 'config', el[config_keys[el['config']]]) # type: ignore[index]

				inc(len(res))

				if directive.excluding_query:

					sids = set(register.keys())
					for el in res:
						if el['stock'] in sids:
							sids.remove(el['stock'])
					if sids:
						for k in sids:
							del register[k]

				for el in res:
					register[el['stock']]['t2'].append(el) # type: ignore[union-attr]

			else:
				raise ValueError(
					f"Unrecognized LoaderDirective: {directive.dict()}"
				)

		if logger and logger.verbose:
			s = f"Unique ids: {len(register)}"
			for col in (col_set - set(["stock"])):
				s += f", {col}: "
				s += str(sum([1 for k in register for el in register[k][col]])) # type: ignore
			logger.info(s)

		return register.values()
