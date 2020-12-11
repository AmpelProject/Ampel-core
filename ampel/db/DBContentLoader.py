#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/DBContentLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 13.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.codec_options import CodecOptions
from typing import Iterable, Union, Dict, Iterator, Optional, Literal

from ampel.type import StockId, ChannelId, StrictIterable, Tag
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.t3.LoaderDirective import LoaderDirective
from ampel.db.FrozenValuesDict import FrozenValuesDict
from ampel.log.utils import safe_query_dict
from ampel.log.AmpelLogger import AmpelLogger
from ampel.db.query.general import build_general_query
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.util.collections import ampel_iter, to_set
from ampel.core.AdminUnit import AdminUnit
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry

freeze_codec_options = CodecOptions(document_class=FrozenValuesDict)

# Monitoring counters
stat_db_loads = AmpelMetricsRegistry.counter(
	"loads",
	"Number of documents loaded",
	subsystem="db",
	labelnames=("col",)
)

class DBContentLoader(AdminUnit):

	logger: Optional[AmpelLogger]

	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]],
		directives: Iterable[LoaderDirective],
		channel: Union[None, ChannelId, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]] = None,
		tag: Optional[Dict[Literal['with', 'without'], Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]]] = None,
		auto_project: bool = True,
		codec_options: CodecOptions = freeze_codec_options,
	) -> Iterable[AmpelBuffer]:
		"""
		:param tag: If specified, query selection critera will apply to all directives.
		:param channel: If specified, query selection critera will apply to all directives (except t0).
		None means all channels are considered (no criterium).

		:param directives: see LoaderDirective docstrings for more information.  Notes:
		- T3 unit configurations can use aliased directives. For example, the alias COMPOUND
		is translated into the directive LoaderDirective(col="t1")
		- LoaderDirective can contain "query_complement", which can be used among other thing to further
		sub-select t2 documents or select given states (such as the latest state)

		:param auto_project: each LoaderDirective is associated with a db collection with is itself associated with
		a default "content" typed dict. By default, we request a projection that projects only the fields defined
		in those TypedDict. Custom/admin fields would thus not be retrieved.
		Set this setting to False if it is not the whished behavior.
		"""

		logger = self.logger if self.logger else AmpelLogger.get_logger()

		if not directives:
			raise ValueError("Parameter 'directives' cannot be empty")

		col_set = {directive.col for directive in directives}

		# Note: the following operation will consume
		# stock_ids if it is an Iterator/Cursor
		register: Dict[StockId, AmpelBuffer] = {
			stock_id: AmpelBuffer(
				id = stock_id,
				t0 = [] if "t0" in col_set else None,
				t1 = [] if "t1" in col_set else None,
				t2 = [] if "t2" in col_set else None,
				log = [] if "log" in col_set else None,
			) for stock_id in ampel_iter(stock_ids)
		}

		for directive in directives:

			query = build_general_query(
				stock=register.keys(), channel=channel, tag=tag
			)

			if directive.col == "stock":
				query['_id'] = query.pop("stock")
			elif directive.col == "t0" and channel:
				query.pop('channel')

			# query 'stock' parameter primes over query complements
			if directive.query_complement:
				query = {**directive.query_complement, **query}

			if logger.verbose > 1: # log query parameters
				logger.debug(
					None, extra={
						'col': directive.col,
						'query': safe_query_dict(query, dict_key=None)
					}
				)

			# Retrieve pymongo cursor
			col = self.context.db.get_collection(directive.col)

			if codec_options:
				col = col.database.get_collection(col.name, codec_options=codec_options)

			# Note: codec_options freezes structures in dicts with depth level > 1
			cursor = col.find(
				filter=query, projection={
					k: 1 for k in directive.model.__annotations__.keys()
				} if auto_project else None
			)

			doc_counter = stat_db_loads.labels(directive.col)

			if directive.col in ("t1", "log"):
				count = 0
				for count, res in enumerate(cursor, 1):
					register[res['stock']][directive.col].append(res) # type: ignore[union-attr]
				doc_counter.inc(count)

			elif directive.col == "stock":
				count = 0
				for count, res in enumerate(cursor, 1):
					register[res['_id']]['stock'] = res
				doc_counter.inc(count)

			# Datapoints are channel-less and can be associated with multiple stocks
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
				doc_counter.inc(count)

			# Potentialy link config dict used by science records
			elif directive.col == "t2":

				# whether to link corresponding run config
				link_config = getattr(directive, 'options') and \
					directive.options.get('link_config', False) # type: ignore[union-attr]

				if link_config:
					config_keys = self.context.config.get('t2.config_keys', dict)
					if not config_keys:
						raise ValueError("Cannot load t2 configs")

				count = 0
				for count, res in enumerate(cursor, 1):

					if link_config:
						# link run_config dict
						res['config'] = res[config_keys[res['config']]] # type: ignore[index]

					register[res['stock']]['t2'].append(res) # type: ignore[union-attr]
				doc_counter.inc(count)

			else:
				raise ValueError(
					f"Unrecognized LoaderDirective: {directive.dict()}"
				)


		if logger.verbose:
			s = f"Unique ids: {len(register)}"
			for col in (col_set - set(["stock"])):
				s += f", {col}: "
				s += str(sum([1 for k in register for el in register[k][col]])) # type: ignore
			logger.info(s)

		return register.values()


	@staticmethod
	def import_journal(tran_data, journal_entries, channels_set, logger):
		"""
		:param tran_data: instance of AmpelBuffer
		:type tran_data: :py:class:`AmpelBuffer <ampel.core.AmpelBuffer>`
		:param list(Dict) journal_entries:
		:param channels_set:
		:type channels_set: set(str), str
		"""

		# Save journal entries related to provided channels
		for entry in journal_entries:

			# Not sure if those entries will exist. Time will tell.
			if entry.get('channel') is None:
				logger.warn(
					'Ignoring following channel-less journal entry: %s' % str(entry),
					extra={'stock': tran_data.tran_id}
				)
				continue

			if channels_set == "Any":
				chans_intersec = "Any"
			else:
				# Set intersection between registered and requested channels (if any)
				chans_intersec = (
					entry['channel'] if channels_set is None
					else (channels_set & to_set(entry['channels']))
				)

			# Removing embedded 'channels' key/value and add journal entry
			# to transient data while maintaining the channels association
			tran_data.add_journal_entry(
				chans_intersec,
				# journal entry without the channels key/value
				{k: v for k, v in entry.items() if k != 'channel'}
			)
