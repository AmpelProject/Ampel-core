#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/include/session/T3SessionLastRunTime.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 09.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, ClassVar
from datetime import datetime, timedelta
from ampel.mongo.query.var.events import get_last_run
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.view.T3Store import T3Store


class T3SessionLastRunTime(AbsT3Supplier[dict]):

	key: ClassVar[str] = "last_run"

	#: timedelta to search for previous run
	lookup_range: Dict[str, int] = {'days': -7}

	#: timedelta to use if no previous run found
	fallback: Optional[Dict[str, int]] = None  # ex: {'days': -90}


	def supply(self, t3s: T3Store) -> dict:
		"""Add last run time (UNIX epoch) as "last_run". """

		last_run = get_last_run(
			self.context.db.get_collection('events'),
			require_success = True,
			process_name = self.event_hdlr.process_name,
			gte_time = self.lookup_range,
			timestamp = True,
		)

		if last_run is None:
			self.logger.warn(f"Event {self.event_hdlr.process_name}: last run time unavailable")
			if self.fallback:
				t = (datetime.now() + timedelta(**self.fallback)).timestamp()
				self.logger.warn(f"Fallback last run time: {t}")
				return {self.key: t}

		return {self.key: last_run}
