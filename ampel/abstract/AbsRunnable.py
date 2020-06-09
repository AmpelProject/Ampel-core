#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsRunnable.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.02.2020
# Last Modified Date: 08.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict
from ampel.base import abstractmethod
from ampel.abstract.AbsAdminUnit import AbsAdminUnit
from ampel.log.LogRecordFlag import LogRecordFlag


class AbsRunnable(AbsAdminUnit, abstract=True):
	"""
	Ampel admin unit featuring method run()

	:param log_flag: Any additional LogRecordFlag (default: SCHEDULED_RUN, possible alternative: MANUAL_RUN)
	:param logger_profile: logger profile key as defined in the ampel conf (ex: 'standard', 'quiet', 'silent')
	:param db_logging_handler_kwargs: optional kwargs to be passed to the DBLoggingHandler constructor.
	:param raise_exc: whether this class should raise Exceptions rather than catching them (default False)
	"""

	process_name: str
	logger_profile: str = "default"
	db_logging_handler_kwargs: Dict[str, Any] = {}

	raise_exc: bool = False
	log_flag: LogRecordFlag = LogRecordFlag.SCHEDULED_RUN


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
