#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/context/T3AddAlertsNumber.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.db.query.events import build_t0_stats_query
from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender
from ampel.t3.context.T3AddLastRunTime import T3AddLastRunTime


class T3AddAlertsNumber(AbsT3RunContextAppender):


	def update(self, run_context: Dict[str, Any]) -> None:
		""" Retrieves the number of alerts processed since last run of this event """

		if 'last_run' not in run_context:
			T3AddLastRunTime(
				context=self.context,
				logger=self.logger,
				process_name=self.process_name
			).update(run_context) # updates context dict

		if not run_context['last_run']:
			self.logger.info(
				"Last run time not available, cannot count number of alerts"
			)
			run_context['processed_alerts'] = None
			return

		# Get number of alerts processed since last run
		res = next(
			self.context.db.get_collection('events').aggregate(
				build_t0_stats_query(gte_time=run_context['last_run'])
			),
			None
		)

		run_context['processed_alerts'] = 0 if res is None else res.get('alerts')
