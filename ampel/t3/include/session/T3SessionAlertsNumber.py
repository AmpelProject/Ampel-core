#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/include/session/T3SessionAlertsNumber.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 09.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import ClassVar
from ampel.mongo.query.var.events import build_t0_stats_query
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.t3.include.session.T3SessionLastRunTime import T3SessionLastRunTime


class T3SessionAlertsNumber(AbsT3Supplier[dict]):
	""" Note: also returns "last run time" of process """

	key: ClassVar[str] = "processed_alerts"


	def supply(self) -> dict:
		""" Returns number of alerts processed since last run of this event as "processed_alerts" """

		d = T3SessionLastRunTime(
			context=self.context,
			logger=self.logger,
			process_name=self.process_name
		).supply()

		if not d[T3SessionLastRunTime.key]:
			self.logger.info(
				"Last run time not available, cannot count number of alerts"
			)
			d[self.key] = None
			return d

		# Get number of alerts processed since last run
		res = next(
			self.context.db.get_collection('events').aggregate(
				build_t0_stats_query(gte_time=d[T3SessionLastRunTime.key])
			),
			None
		)

		d[self.key] = 0 if res is None else res.get('alerts')
		return d
