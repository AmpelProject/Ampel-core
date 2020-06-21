#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsProcessorUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.02.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict, Optional
from ampel.type import ChannelId
from ampel.base import abstractmethod
from ampel.core.AdminUnit import AdminUnit
from ampel.log.LogRecordFlag import LogRecordFlag


class AbsProcessorUnit(AdminUnit, abstract=True):
	"""
	Ampel admin unit featuring methods run (abstract), new_run_id and dedicated attributes

	:param base_log_flag: Any additional LogRecordFlag (default: SCHEDULED_RUN, possible alternative: MANUAL_RUN)
	:param log_profile: logger profile key as defined in the ampel conf (ex: 'standard', 'quiet', 'silent')
	:param db_handler_kwargs: optional override arguments to be passed to the DBLoggingHandler constructor
	:param raise_exc: whether this class should raise Exceptions rather than catching them (default False)
	"""

	process_name: str
	channel: Optional[ChannelId] = None

	raise_exc: bool = False
	log_profile: str = "default"
	base_log_flag: LogRecordFlag = LogRecordFlag.SCHEDULED_RUN
	db_handler_kwargs: Optional[Dict[str, Any]] = None


	@abstractmethod(var_args=True)
	def run(self) -> Any:
		...


	def new_run_id(self) -> int:
		"""
		run_id is a global (ever increasing) counter stored in the DB
		used to tie log entries from the same process with each other
		"""
		return self.context.db \
			.get_collection('counter') \
			.find_one_and_update(
				{'_id': 'current_run_id'},
				{'$inc': {'value': 1}},
				new=True, upsert=True
			) \
			.get('value')
