#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/context/T3AddLastRunTime.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 10.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from ampel.db.query.events import get_last_run
from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender


class T3AddLastRunTime(AbsT3RunContextAppender):

	lookup_range: Dict[str, int] = {'days': -7}
	fall_back: Optional[Dict[str, int]] = None  # ex: {'days': -90}

	def update(self, context: Dict[str, Any]) -> None:

		if 'last_run' in context:
			self.logger.info("Field 'last_run' is already set in context dict")
			return

		last_run = get_last_run(
			self.context.db.get_collection('events'),
			process_name = self.process_name,
			gte_time = self.lookup_range
		)

		if last_run is None:
			self.logger.warn(f"Event {self.process_name}: last run time unavailable")
			if self.fall_back:
				context['last_run'] = (datetime.now() + timedelta(**self.fall_back)).timestamp()
				self.logger.warn(f"Fallback last run time: {context['last_run']}")
				return

		context['last_run'] = last_run
