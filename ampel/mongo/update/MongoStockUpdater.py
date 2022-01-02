#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/MongoStockUpdater.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                15.10.2018
# Last Modified Date:  03.09.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from bson import ObjectId
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateMany, UpdateOne
from typing import Any, Literal, get_args
from collections.abc import Sequence

from ampel.core.AmpelDB import AmpelDB
from ampel.types import ChannelId, Tag, StockId
from ampel.log import AmpelLogger, VERBOSE
from ampel.log.utils import report_exception
from ampel.mongo.utils import maybe_use_each
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.content.JournalRecord import JournalRecord


tag_type = get_args(Tag) # type: ignore[misc]
chan_type = get_args(ChannelId) # type: ignore[misc]


class MongoStockUpdater:

	def __init__(self,
		ampel_db: AmpelDB, tier: Literal[-1, 0, 1, 2, 3], run_id: int,
		process_name: str, logger: AmpelLogger, raise_exc: bool = False,
		bump_updated: bool = True, update_journal: bool = True,
		extra_tag: None | Tag | Sequence[Tag] = None,
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
		self.bump_updated = bump_updated
		self.auto_flush = auto_flush
		self.process_name = process_name
		self.extra_tag = extra_tag
		self.logger = logger
		self.reset()


	def reset(self) -> None:
		self._updates: list[UpdateOne | UpdateMany] = []
		self._one_updates: dict[StockId, UpdateOne] = {}
		self._multi_updates: dict[StockId, list[UpdateMany]] = {}


	def new_journal_record(self,
		unit: None | int | str = None,
		channels: None | ChannelId | Sequence[ChannelId] = None,
		action_code: None | JournalActionCode = None,
		doc_id: None | ObjectId = None,
		now: None | int | float = None
	) -> JournalRecord:

		ret: JournalRecord = {
			'tier': self.tier,
			'ts': now if now else int(time()),
			'process': self.process_name,
			'run': self.run_id,
			'action': action_code or JournalActionCode(0)
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


	def add_journal_record(self,
		stock: StockId | Sequence[StockId],
		jattrs: None | JournalAttributes = None,
		tag: None | Tag | Sequence[Tag] = None,
		name: None | str | Sequence[str] = None,
		trace_id: None | dict[str, int] = None,
		action_code: None | JournalActionCode = None,
		doc_id: None | ObjectId = None,
		unit: None | int | str = None,
		channel: None | ChannelId | Sequence[ChannelId] = None,
		now: None | int | float = None
	) -> JournalRecord:
		"""
		:returns: the JournalRecord dict instance associated with the stock document(s) update
		is returned by this method so that customization can be made if necessary.
		Note that these customizations must be made before additional operations such as add_tag.
		Note that the associated update operation does more than just adding a journal record,
		it also modifies the "updated" field of stock document(s).
		"""

		jrec = self.new_journal_record(unit, channel, action_code, doc_id, now)

		if jattrs:

			if jattrs.tag:
				self.include_jtags(jrec, jattrs.tag)

			if jattrs.extra:
				jrec['extra'] = jattrs.extra

			if jattrs.code and jattrs.code > 0:
				jrec['code'] = jattrs.code

		if trace_id:
			jrec['traceid'] = trace_id

		upd: dict[str, Any] = {'$push': {'journal': jrec}}

		if tag or name:

			upd['$addToSet'] = (
				({'tag': tag if isinstance(tag, (str, int)) else maybe_use_each(tag)} if tag else {}) |
				({'name': name if isinstance(name, str) else maybe_use_each(name)} if name else {})
			)

		if self.bump_updated:

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

			# Current strategy:
			# - case: UpdateMany + UpdateMany: do nothing or put differently: submit the two ops as is
			# - case: UpdateOne + UpdateMany: update op UpdateOne and no longer match target stock in UpdateMany
			# - case: UpdateOne + UpdateOne: merge updates
			if not isinstance(stock, (int, str)):
				self._add_many_update(list(stock), upd)
			else:
				self._add_one_update(stock, upd)

		return jrec


	def add_name(self, stock: StockId, name: str | Sequence[str]) -> None:
		self._add_one_update(
			stock,
			{'$addToSet': {'name': name if isinstance(name, str) else {'$each': name}}}
		)


	def add_tag(self,
		stock: StockId | Sequence[StockId],
		tag: Tag | Sequence[Tag]
	) -> None:

		upd = {'$addToSet': {'tag': tag if isinstance(tag, tag_type) else {'$each': tag}}}

		if isinstance(stock, (int, bytes, str)): # StockId
			self._add_one_update(stock, upd)
		else: # Sequence[StockId]
			self._add_many_update(stock if isinstance(stock, list) else list(stock), upd)


	def _add_one_update(self, stock: StockId, upd: dict[str, Any]) -> None:

		uo = UpdateOne({'stock': stock}, upd)

		if stock in self._multi_updates:
			for um in self._multi_updates.pop(stock):
				um._filter['stock']['$in'].remove(stock)
				self._merge_updates(uo, um._doc)

		if stock in self._one_updates:
			self._merge_updates(self._one_updates[stock], upd)
		else:
			self._one_updates[stock] = uo
			self._updates.append(uo)

			if self.auto_flush and len(self._updates) > self.auto_flush:
				self.flush()


	def _add_many_update(self, stocks: list[StockId], upd: dict[str, Any]) -> None:

		um = UpdateMany({'stock': {'$in': stocks}}, upd)

		for s in iter(stocks):
			if s in self._one_updates:
				self._merge_updates(self._one_updates[s], upd)
				stocks.remove(s)

		if stocks:

			for s in stocks:
				if s in self._multi_updates:
					self._multi_updates[s].append(um)
				else:
					self._multi_updates[s] = [um]

			self._updates.append(um)
			if self.auto_flush and len(self._updates) > self.auto_flush:
				self.flush()


	def _merge_updates(self, op: UpdateOne, d: dict) -> None:
		"""
		modifies provided UpdateOne structure
		:raises: ValueError in case update structures are not conform ex: {'$addToSet': {'name': {'a': 1}}}
		"""

		opd = op._doc

		if '$max' in d:
			if '$max' in opd:
				for k in d['$max']:
					opd['$max'][k] = max(opd['$max'][k], d['$max'][k])
			else:
				opd['$max'] = d['$max']

		if '$set' in d:
			if '$set' in opd:
				for k in d['$set']:
					# Second update prevails as would occur if there was two ops
					opd['$set'][k] = d['$set'][k]
			else:
				opd['$set'] = d['$set']


		for mop in ('$push', '$addToSet'):
			if mop in d:
				if mop in opd:
					for k in d[mop]:
						if k in opd[mop]:
							if isinstance(opd[mop][k], str) or (isinstance(opd[mop][k], dict) and '$each' not in opd[mop][k]):
								opd[mop][k] = {'$each': [opd[mop][k]]}
							if isinstance(d[mop][k], str):
								opd[mop][k]['$each'].append(d[mop][k])
							elif isinstance(d[mop][k], dict):
								opd[mop][k]['$each'].append(d[mop][k]['$each'] if '$each' in d[mop][k] else d[mop][k])
							else:
								raise ValueError(f"Unrecognized d[{mop}][{k}] value: {d[mop][k]}")
						else:
							opd[mop][k] = d[mop][k]
				else:
					opd[mop] = d[mop]

		for k in d.keys() - ['$set', '$max', '$push', '$addToSet']:
			opd[k] = d[k]


	def flush(self) -> None:

		if not self._updates:
			return

		jupds = self._updates
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

			info: dict[str, Any] = {
				'process': self.process_name,
				'msg': 'Exception in flush()',
				'journalUpdateCount': len(jupds)
			}

			if isinstance(e, BulkWriteError):
				info['BulkWriteError'] = str(e.details)

			# Populate troubles collection
			report_exception(self._ampel_db, self.logger, exc=e, info=info)


	@staticmethod
	def include_jtags(jrec: JournalRecord, tag: Tag | Sequence[Tag]):
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
