#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/context/T3AddLastRunTime.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 06.08.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, ClassVar
from datetime import datetime, timedelta
from ampel.db.query.events import get_last_run
from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender


class T3AddLastRunTime(AbsT3RunContextAppender):

	key: ClassVar[str] = "last_run"

	#: timedelta to search for previous run
	lookup_range: Dict[str, int] = {'days': -7}
	#: timedelta to use if no previous run found
	fallback: Optional[Dict[str, int]] = None  # ex: {'days': -90}

	def update(self, context: Dict[str, Any]) -> None:
		"""Add last run time (UNIX epoch) as "last_run". """
		if self.key in context:
			self.logger.info(f"Field '{self.key}' is already set in context dict")
			return

		last_run = get_last_run(
			self.context.db.get_collection('events'),
			require_success = True,
			process_name = self.process_name,
			gte_time = self.lookup_range
		)

		if last_run is None:
			self.logger.warn(f"Event {self.process_name}: last run time unavailable")
			if self.fallback:
				context[self.key] = (datetime.now() + timedelta(**self.fallback)).timestamp()
				self.logger.warn(f"Fallback last run time: {context[self.key]}")
				return

		context[self.key] = last_run
