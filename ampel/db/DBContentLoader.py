#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/DBContentLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 13.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.codec_options import CodecOptions
from typing import Iterable, Union, Dict, Iterator, Optional

from ampel.type import StockId, ChannelId, StrictIterable
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.t3.LoaderDirective import LoaderDirective
from ampel.db.FrozenValuesDict import FrozenValuesDict
from ampel.log.utils import safe_query_dict
from ampel.log.AmpelLogger import AmpelLogger
from ampel.query.QueryUtils import QueryUtils
from ampel.t3.SnapData import SnapData
from ampel.util.collections import ampel_iter, to_set
from ampel.abstract.AbsAdminUnit import AbsAdminUnit

freeze_codec_options = CodecOptions(
	document_class=FrozenValuesDict
)

class DBContentLoader(AbsAdminUnit):

	logger: Optional[AmpelLogger]

	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]],
		directives: Iterable[LoaderDirective],
		channels: Union[None, ChannelId, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]] = None,
		codec_options = freeze_codec_options
	) -> Iterable[SnapData]:
		"""
		:param channels: None means all channels are considered (no criterium).
		:param directives: see LoaderDirective docstrings for more information.  Notes:
			- T3 unit configurations can use aliased directives.
			For example, the alias COMPOUND is translated into the directive
			LoaderDirective(col="t1")
			- LoaderDirective can contain "query_complement", which can be used
			among other thingd to further sub-select t2 documents or select given states
			(such as the latest state)
		"""

		logger = self.logger if self.logger else AmpelLogger.get_logger()

		if not directives:
			raise ValueError("Parameter 'directives' cannot be empty")

		col_set = {directive.col for directive in directives}

		# Note: the following operation will consume
		# stock_ids if it is an Iterator/Cursor
		register: Dict[StockId, SnapData] = {
			stock_id: SnapData(
				id = stock_id,
				t0 = [] if "t0" in col_set else None,
				t1 = [] if "t1" in col_set else None,
				t2 = [] if "t2" in col_set else None,
				logs = [] if "logs" in col_set else None,
			) for stock_id in ampel_iter(stock_ids)
		}

		for directive in directives:

			query = {'stock': QueryUtils.match_array(register.keys())}

			if directive.col == "stock":
				query['_id'] = query.pop("stock")

			# query 'stock' parameter primes over query complements
			if directive.query_complement:
				query = {**directive.query_complement, **query}

			if logger.verbose > 1: # log query parameters
				logger.debug(
					None, extra={
						'col': directive.col,
						'stock': list(register.keys()),
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
				}
			)

			if directive.col in ("t1", "logs"):
				for res in cursor:
					register[res['stock']][directive.col].append(res) # type: ignore[union-attr]

			elif directive.col == "stock":
				for res in cursor:
					register[res['_id']]['stock'] = res

			# Datapoints are channel-less and can be associated with multiple stocks
			elif directive.col == "t0":

				for res in cursor:

					# Upper limits can be attached to multiple transients
					for sid in res['stock']:

						# Some of which might not match with our query
						if sid not in register:
							continue

						# Add datapoints to snapdata (no need to recursive_freeze
						# since dict elements with level > 1 are frozen due to codec_options
						register[sid]['t0'].append(res) # type: ignore[union-attr]

			# Potentialy link config dict used by science records
			elif directive.col == "t2":

				# whether to link corresponding run config
				link_config = getattr(directive, 'options') and \
					directive.options.get('link_config', False) # type: ignore[union-attr]

				if link_config:
					config_keys = self.context.config.get('t2.config_keys', dict)
					if not config_keys:
						raise ValueError("Cannot load t2 configs")

				for res in cursor:

					if link_config:
						# link run_config dict
						res['config'] = res[config_keys[res['config']]] # type: ignore[index]

					register[res['stock']]['t2'].append(res) # type: ignore[union-attr]

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
		:param tran_data: instance of SnapData
		:type tran_data: :py:class:`SnapData <ampel.t3.SnapData>`
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
