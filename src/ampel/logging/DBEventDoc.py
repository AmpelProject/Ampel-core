#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/logging/DBEventDoc.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 04.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time, strftime
from typing import Dict, Union
from ampel.db.AmpelDB import AmpelDB
from ampel.logging.AmpelLoggingError import AmpelLoggingError

class DBEventDoc():
	"""
	"""

	default_doc_id = '%Y%m%d'

	def __init__(
		self, ampel_db: AmpelDB, event_name: str, tier: int, ts: int = None, 
		col_name: str = "events", doc_id: str = None
	):
		""" 
		:param event_name: event name. For example 'ap' (alertprocessor) or task name
		:param tier: value (0,1,2,3) indicating at which ampel tier level logging is done
		:param col_name: name of db collection to use (default 'events'). 
		:param doc_id: optional string that will be provided as argument to method `strftime` (default: '%Y%m%d')
		:param ts: timestamp
		"""
		self.event_name = event_name
		self.tier = tier
		self.col = ampel_db.get_collection(col_name)
		self.run_ids = []
		self.doc_id = doc_id if doc_id else DBEventDoc.default_doc_id
		self.event_info = {}
		self.ts = int(time()) if ts is None else int(ts)
		self.duration = 0


	def set_event_info(self, event_info: Dict) -> None:
		""" 
		:param dict event_info:
		:returns: None
		"""
		self.event_info = event_info


	def add_run_id(self, run_id: int) -> None:
		"""
		:param int run_id: 
		:returns: None
		"""
		self.run_ids.append(run_id)


	def add_duration(self, seconds: Union[float, int]) -> None:
		"""
		:param seconds:
		"""
		self.duration += seconds


	def publish(self) -> None:
		""" 
		:raises: AmpelLoggingError
		"""

		# Record event info into DB
		res = self.col.update_one(
			{
				'_id': int(strftime(self.doc_id))
			},
			{
				'$push': {
					'events': {
						'event': self.event_name,
						'tier': self.tier,
						'ts': self.ts,
						'duration': int(time()-self.ts) if self.duration == 0 else int(self.duration),
						'runId': self.run_ids[0] if len(self.run_ids) == 1 else self.run_ids,
						**self.event_info
					}
				}
			},
			upsert=True
		)

		if res.modified_count == 0 and res.upserted_id is None:
			raise AmpelLoggingError(
				"Events collection update failed (%s)" % {
					'mongoUpdateResult': res.raw_result,
					'event': self.event_name
				}
			)

		self.event_info = {}
