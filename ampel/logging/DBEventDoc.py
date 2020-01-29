#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/DBEventDoc.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 06.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time, strftime
from typing import Dict, Union, List, Any, Optional, Literal, Iterable
from ampel.db.AmpelDB import AmpelDB
from ampel.logging.AmpelLoggingError import AmpelLoggingError
from ampel.common.AmpelUtils import AmpelUtils


class DBEventDoc:
	"""
	Class handling the creation and publication of event documents into the event database
	"""

	default_id_format = '%Y%m%d'

	def __init__(self, 
		ampel_db: AmpelDB, event_name: str, tier: Literal[0, 1, 2, 3], 
		ts: Optional[int] = None, col_name: str = "events", id_format: Optional[str] = None
	):
		"""
		:param event_name: event name. For example 'ap' (alertprocessor) or process name
		:param tier: indicates at which tier level logging is done
		:param col_name: name of db collection to use (default 'events'). 
		:param id_format: optional string that will be provided as argument to method `strftime` (default: '%Y%m%d')
		:param ts: provided timestamp will be used as 'start' time rather than current time
		"""
		self.event_name = event_name
		self.tier = tier
		self.col = ampel_db.get_collection(col_name)
		self.run_ids: List[int] = []
		self.id_format = id_format if id_format else self.default_id_format
		self.event_info: Dict[str, Any] = {}
		self.ts = int(time()) if ts is None else int(ts)
		self.duration = 0.0


	def set_event_info(self, event_info: Dict) -> None:
		""" """
		self.event_info = event_info


	def add_run_id(self, run_ids: Union[int, Iterable[int]]) -> None:
		"""
		Multiple run ids can be provided at once
		"""
		for run_id in AmpelUtils.iter(run_ids):
			self.run_ids.append(run_id)


	def add_duration(self, seconds: Union[float, int]) -> None:
		""" 
		Add seconds to the internal 'duration' buffer. 
		If you use this method, the final 'duration' of the process associated with 
		this event doc will be the sum of the values provided with add_duration
		rather than the default time interval between class instantiation 
		and the final call to the method publish()
		"""
		self.duration += seconds


	def publish(self) -> None:
		""" 
		:raises: AmpelLoggingError
		"""

		# Record event info into DB
		res = self.col.update_one(
			{
				'_id': int(strftime(self.id_format))
			},
			{
				'$push': {
					'events': {
						'process': self.event_name,
						'tier': self.tier,
						'ts': self.ts,
						'duration': int(time()-self.ts) if self.duration == 0 else int(self.duration),
						'run': self.run_ids[0] if len(self.run_ids) == 1 else self.run_ids,
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
					'process': self.event_name
				}
			)

		self.event_info = {}
