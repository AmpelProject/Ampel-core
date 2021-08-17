#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/session/T3SessionAlertsNumber.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 23.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, ClassVar
from ampel.mongo.query.var.events import build_t0_stats_query
from ampel.abstract.AbsSessionInfo import AbsSessionInfo
from ampel.t3.session.T3SessionLastRunTime import T3SessionLastRunTime


class T3SessionAlertsNumber(AbsSessionInfo):

	key: ClassVar[str] = "processed_alerts"

	def update(self, run_context: Dict[str, Any]) -> None:
		""" Add number of alerts processed since last run of this event as "processed_alerts" """

		if T3SessionLastRunTime.key not in run_context:
			T3SessionLastRunTime(
				context=self.context,
				logger=self.logger,
				process_name=self.process_name
			).update(run_context) # updates context dict

		if not run_context[T3SessionLastRunTime.key]:
			self.logger.info(
				"Last run time not available, cannot count number of alerts"
			)
			run_context[self.key] = None
			return

		# Get number of alerts processed since last run
		res = next(
			self.context.db.get_collection('events').aggregate(
				build_t0_stats_query(gte_time=run_context[T3SessionLastRunTime.key])
			),
			None
		)

		run_context[self.key] = 0 if res is None else res.get('alerts')
