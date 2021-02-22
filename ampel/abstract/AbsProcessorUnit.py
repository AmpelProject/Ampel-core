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
from ampel.log.LogFlag import LogFlag


class AbsProcessorUnit(AdminUnit, abstract=True):
	"""
	Base class for units that orchestrate an Ampel processing tier.
	"""

	#: name of the process that instantiated this processor
	process_name: str
	#: channels associated with this processor
	channel: Optional[ChannelId] = None

	#: raise exceptions instead of catching and logging
	raise_exc: bool = False
	#: logging configuration to use, from the logging section of the Ampel config
	log_profile: str = "default"
	#: flag to apply to all log records created
	base_log_flag: LogFlag = LogFlag.SCHEDULED_RUN
	#: additional kwargs to pass to :class:`~ampel.log.handlers.DBLoggingHandler.DBLoggingHandler`
	db_handler_kwargs: Optional[Dict[str, Any]] = None


	@abstractmethod(var_args=True)
	def run(self) -> Any:
		"""
		Run processing steps.
		"""
		...


	def new_run_id(self) -> int:
		"""
		Return an identifier that can be used to associate log entries from a
		single process invocation. This ID is unique and monotonicaly increasing.
		"""
		return self.context.db \
			.get_collection('counter') \
			.find_one_and_update(
				{'_id': 'current_run_id'},
				{'$inc': {'value': 1}},
				new=True, upsert=True
			) \
			.get('value')
