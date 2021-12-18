#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/EventHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from bson import ObjectId
from typing import Any, Optional, Literal, TYPE_CHECKING
from ampel.content.EventDocument import EventDocument
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.AmpelLoggingError import AmpelLoggingError
from ampel.log.utils import report_exception

if TYPE_CHECKING:
	from ampel.core.AmpelDB import AmpelDB


# TODO: (much later) remove explicit dependency on pymongo
class EventHandler:
	"""
	Handles the creation and publication of event documents into the event database
	"""

	def __init__(self,
		process_name: str,
		ampel_db: 'AmpelDB',
		tier: Literal[-1, 0, 1, 2, 3],
		run_id: int,
		col_name: str = "events",
		extra: Optional[dict[str, Any]] = None,
		raise_exc = False,
		dry_run: bool = False
	):
		"""
		:param col_name: name of db collection to use (default 'events').
		"""

		self.process_name = process_name
		self.db = ampel_db
		self.raise_exc = raise_exc
		self.dry_run = dry_run
		self.run_id = run_id
		doc = EventDocument(process=process_name, tier=tier)

		if run_id:
			doc['run'] = run_id

		if extra:
			doc |= extra # type: ignore[assignment]

		self.dkeys = doc.keys()
		self.extra: Optional[dict[str, Any]] = None

		if dry_run:
			self.col = None
			self.ins_id = ObjectId()
		else:
			self.col = ampel_db.get_collection(col_name)
			self.ins_id = self.col.insert_one(doc).inserted_id


	def get_run_id(self) -> int:
		return self.run_id


	def add_extra(self, overwrite: bool = False, **extra) -> None:

		if self.extra is None:
			self.extra = extra
			return

		for k, v in extra.items():
			if k in self.extra and not overwrite:
				continue
			self.extra[k] = v


	def handle_error(self, e: Exception, logger: AmpelLogger) -> None:

		self.add_extra(overwrite=True, success=False)

		if self.raise_exc:
			raise e

		report_exception(
			self.db, logger, exc=e,
			info={'process': self.process_name}
		)


	def update(self, logger: AmpelLogger, save_duration: bool = True, **kwargs) -> None:
		""" :raises: AmpelLoggingError """

		upd: dict[str, Any] = {}

		if self.extra:
			for k, v in self.extra.items():
				if k in self.dkeys:
					logger.error(f"Cannot overwrite already existing event value for key {k}")
					continue
				upd[k] = v

		for k, v in kwargs.items():
			if k in self.dkeys:
				logger.error(f"Cannot overwrite already existing event value for key {k}")
				continue
			upd[k] = v

		if save_duration:
			upd['duration'] = round(time() - self.ins_id.generation_time.timestamp(), 3)
		elif not upd:
			return

		if not self.dry_run:
			res = self.col.update_one({'_id': self.ins_id}, {'$set': upd}) # type: ignore[union-attr]

			if res.modified_count == 0 and res.upserted_id is None:
				raise AmpelLoggingError(
					"Events collection update failed (%s)" % {
						'mongoUpdateResult': res.raw_result,
						'process': self.process_name
					}
				)
