#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/StockJournalUpdater.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 22.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from bson import ObjectId
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateMany, UpdateOne
from typing import List, Any, Optional, Sequence, Union, Dict, Literal, get_args

from ampel.core.AmpelDB import AmpelDB
from ampel.types import ChannelId, Tag, StockId
from ampel.log import AmpelLogger, VERBOSE
from ampel.log.utils import report_exception
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.content.JournalRecord import JournalRecord

tag_type = get_args(Tag) # type: ignore[misc]
chan_type = get_args(ChannelId) # type: ignore[misc]

class StockJournalUpdater:

	def __init__(self,
		ampel_db: AmpelDB, tier: Literal[-1, 0, 1, 2, 3], run_id: int,
		process_name: str, logger: AmpelLogger, raise_exc: bool = False,
		update_updated: bool = True, update_journal: bool = True,
		extra_tag: Optional[Union[Tag, Sequence[Tag]]] = None,
		auto_flush: int = 0
	) -> None:
		"""
		:param raise_exc: raise exception rather than populating 'troubles' collection
		"""

		self._ampel_db = ampel_db
		self.col_stock = ampel_db.get_collection('stock')
		self.run_id = run_id
		self.tier = tier
		self.raise_exc = raise_exc
		self.update_journal = update_journal
		self.update_updated = update_updated
		self.auto_flush = auto_flush
		self.process_name = process_name
		self.extra_tag = extra_tag
		self.logger = logger
		self.reset()


	def reset(self) -> None:
		self.journal_updates: List[Any] = []
		self.journal_updates_count = 0


	def new_record(self,
		unit: Optional[Union[int, str]] = None,
		channels: Optional[Union[ChannelId, Sequence[ChannelId]]] = None,
		doc_id: Optional[ObjectId] = None,
		now: Optional[Union[int, float]] = None
	) -> JournalRecord:

		ret: JournalRecord = {
			'tier': self.tier,
			'ts': now if now else int(time()),
			'process': self.process_name,
			'run': self.run_id
		}

		if channels:
			ret['channel'] = channels

		if unit:
			ret['unit'] = unit

		if doc_id:
			ret['doc'] = doc_id.binary

		if self.extra_tag:
			ret['tag'] = self.extra_tag

		return ret


	def add_record(self,
		stock: Union[StockId, Sequence[StockId]],
		jattrs: Optional[JournalAttributes] = None,
		trace_id: Optional[Dict[str, int]] = None,
		doc_id: Optional[ObjectId] = None,
		unit: Optional[Union[int, str]] = None,
		channel: Optional[Union[ChannelId, Sequence[ChannelId]]] = None,
		now: Optional[Union[int, float]] = None
	) -> JournalRecord:
		"""
		:returns: the JournalRecord dict instance associated with the stock document(s) update
		is returned by this method so that customization can be made if necessary.
		Note that the associated update operation does more than just adding a journal record,
		it also modifies the "updated" field of stock document(s).
		"""

		if isinstance(stock, Sequence):
			self.journal_updates_count += len(stock)
			match: Any = {'$in': list(stock)}
			Op = UpdateMany
		else:
			self.journal_updates_count += 1
			match = stock
			Op = UpdateOne

		jrec = self.new_record(unit, channel, doc_id, now)

		if jattrs:

			if jattrs.tag:
				self.include_tags(jrec, jattrs.tag)

			if jattrs.extra:
				jrec['extra'] = jattrs.extra

			if jattrs.code and jattrs.code > 0:
				jrec['code'] = jattrs.code

		if trace_id:
			jrec['traceid'] = trace_id

		upd: dict[str,Any] = {'$push': {'journal': jrec}}

		if self.update_updated:

			upd['$max'] = {'ts.any.upd': jrec['ts']}

			if channel:
				if isinstance(channel, chan_type):
					upd['$max'][f'ts.{channel}.upd'] = jrec['ts']
				else:
					upd['$max'].update({
						f'ts.{chan}.upd': jrec['ts']
						for chan in channel # type: ignore[union-attr]
					})

		if self.update_journal:

			self.journal_updates.append(
				Op({'stock': match}, upd)
			)

			if self.auto_flush and len(self.journal_updates) > self.auto_flush:
				self.flush()

		return jrec


	def flush(self) -> None:

		if not self.journal_updates:
			return

		jupds = self.journal_updates
		self.reset()

		try:

			if self.logger.verbose > 1:
				for el in jupds:
					self.logger.log(VERBOSE, f"Journal update: {str(el)}")

			self.col_stock.bulk_write(jupds)

			if self.logger.verbose:
				self.logger.log(VERBOSE,
					f"{len(jupds)} journal entr{'ies' if len(jupds) > 1 else 'y'} inserted"
				)

		except Exception as e:

			if self.raise_exc:
				if isinstance(e, BulkWriteError):
					raise ValueError(f"Journal update error: {e.details}")
				raise ValueError("Journal update error")

			info: Dict[str, Any] = {
				'process': self.process_name,
				'msg': 'Exception in flush()',
				'journalUpdateCount': self.journal_updates_count
			}

			if isinstance(e, BulkWriteError):
				info['BulkWriteError'] = str(e.details)

			# Populate troubles collection
			report_exception(self._ampel_db, self.logger, exc=e, info=info)


	@staticmethod
	def include_tags(jrec: JournalRecord, tag: Union[Tag, Sequence[Tag]]):
		""" Modifies the input JournalRecord dict """

		if tag:

			# journal record also contains tag(s)
			if 'tag' in jrec:

				# tweak request is about a single tag (not a sequence of tags)
				if isinstance(tag, tag_type):
					# journal record contains single tag (not a sequence of tags)
					if isinstance(jrec['tag'], tag_type):
						jrec['tag'] = [jrec['tag'], tag] # type: ignore[list-item]
					else:
						jrec['tag'].append(tag) # type: ignore[union-attr]

				# multi-tag tweak request
				else:
					# journal record contains single tag (not a sequence of tags)
					if isinstance(jrec['tag'], tag_type):
						jrec['tag'] = [*jrec['tag'], tag] # type: ignore[list-item, misc]
					else:
						jrec['tag'] = jrec['tag'] + tag # type: ignore[operator]

			# journal record contains no tag
			else:
				jrec['tag'] = tag
