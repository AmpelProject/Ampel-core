#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/EventHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 31.08.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Dict, Any, Optional, Literal, TYPE_CHECKING
from ampel.content.EventDocument import EventDocument
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.AmpelLoggingError import AmpelLoggingError

if TYPE_CHECKING:
	from ampel.core.AmpelDB import AmpelDB

class EventHandler:
	""" Handles the creation and publication of event documents into the event database """

	def __init__(self,
		ampel_db: 'AmpelDB', process_name: str, tier: Literal[-1, 0, 1, 2, 3],
		run_id: Optional[int] = None, col_name: str = "events", extra: Optional[Dict[str, Any]] = None
	):
		"""
		:param col_name: name of db collection to use (default 'events').
		"""

		self.process_name = process_name
		self.col = ampel_db.get_collection(col_name)
		doc = EventDocument(process=process_name, tier=tier)

		if run_id:
			doc['run'] = run_id

		if extra:
			doc = {**extra, **doc} # type: ignore[misc]

		self.dkeys = doc.keys()
		self.extra: Optional[Dict[str, Any]] = None
		self.ins_id = self.col.insert_one(doc).inserted_id


	def add_extra(self, overwrite: bool = False, **extra) -> None:

		if self.extra is None:
			self.extra = extra
			return

		for k, v in extra.items():
			if k in self.extra and not overwrite:
				continue
			self.extra[k] = v


	def update(self, logger: AmpelLogger, save_duration: bool = True, **kwargs) -> None:
		""" :raises: AmpelLoggingError """

		upd: Dict[str, Any] = {}

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

		res = self.col.update_one({'_id': self.ins_id}, {'$set': upd})

		if res.modified_count == 0 and res.upserted_id is None:
			raise AmpelLoggingError(
				"Events collection update failed (%s)" % {
					'mongoUpdateResult': res.raw_result,
					'process': self.process_name
				}
			)
