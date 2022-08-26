#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/EventHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.09.2018
# Last Modified Date:  26.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from bson import ObjectId
from typing import Any, Literal, TYPE_CHECKING
from ampel.enum.EventCode import EventCode
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
		col_name: str = "event",
		raise_exc = False,
		dry_run: bool = False,
		job_sig: None | int = None,
		extra: None | dict[str, Any] = None
	):
		"""
		:param col_name: name of db collection to use (default 'events').
		"""
		self.process_name = process_name
		self.db = ampel_db
		self.raise_exc = raise_exc
		self.dry_run = dry_run
		self.job_sig = job_sig
		self.dry_run = dry_run
		self.run_id: None | int = None
		self.code: None | EventCode = None
		self.col = ampel_db.get_collection(col_name)
		self.extra: dict[str, Any] = extra or {}


	def register(self,
		run_id: None | int = None,
		tier: None | Literal[-1, 0, 1, 2, 3] = None,
		task_nbr: None | int = None,
		code: EventCode = EventCode.RUNNING
	) -> None:

		doc = {'process': self.process_name} | self.extra
		doc['code'] = code

		if tier:
			doc['tier'] = tier

		if run_id is not None:
			self.set_run_id(run_id)
			doc['run'] = run_id

		if self.job_sig:
			doc['jobid'] = self.job_sig

		self.dkeys = doc.keys()
		self.extra = {}

		if self.dry_run:
			self.ins_id = ObjectId()
		else:
			self.ins_id = self.col.insert_one(doc).inserted_id # type: ignore[arg-type]


	def set_run_id(self, val: int) -> None:
		if self.run_id is not None:
			raise ValueError("run id already set")
		self.run_id = val


	def get_run_id(self) -> int:
		if self.run_id is None:
			raise ValueError("run id not set")
		return self.run_id


	def add_extra(self, overwrite: bool = False, logger: None | AmpelLogger = None, **extra) -> None:
		for k, v in extra.items():
			if (k in self.extra or k in self.dkeys) and not overwrite:
				if logger:
					logger.error(f"Cannot overwrite already existing event value for key {k}")
				continue
			self.extra[k] = v


	def set_tier(self, val: Literal[0, 1, 2, 3]) -> None:
		self.extra['tier'] = val


	def set_code(self, val: EventCode):
		if self.code == EventCode.EXCEPTION and val != EventCode.EXCEPTION:
			raise ValueError("Cannot override EventCode.EXCEPTION")
		self.code = val


	def handle_error(self, e: Exception, logger: AmpelLogger) -> None:

		self.code = EventCode.EXCEPTION

		if self.raise_exc:
			raise e

		# Try to insert doc into trouble collection (raises no exception)
		# Exception will be logged out to console
		report_exception(
			self.db, logger, exc=e,
			info={'process': self.process_name}
		)


	def update(self, _save_duration: bool = True) -> None:
		""" :raises: AmpelLoggingError """

		upd: dict[str, Any] = self.extra.copy()
		if self.code is not None:
			upd['code'] = self.code

		if _save_duration:
			upd['duration'] = round(time() - self.ins_id.generation_time.timestamp(), 3)

		if not upd or self.dry_run:
			return

		res = self.col.update_one( # type: ignore[union-attr]
			{'_id': self.ins_id},
			{'$set': upd}
		)

		if res.modified_count == 0 and res.upserted_id is None:
			raise AmpelLoggingError(
				"Events collection update failed (%s)" % {
					'mongoUpdateResult': res.raw_result,
					'process': self.process_name
				}
			)
