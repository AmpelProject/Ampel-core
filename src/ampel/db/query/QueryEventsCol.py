#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/query/QueryEventsCol.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.07.2018
# Last Modified Date: 05.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time, strftime, gmtime

class QueryEventsCol:
	"""
	"""

	@staticmethod
	def get_last_run(event_name, days_back=10):
		"""
		:param str event_name:
		:param int days_back:
		"""

		ret = QueryEventsCol.get(
			tier=3, event_name=event_name, days_back=days_back
		)

		ret.append(
			{'$limit': 1} # take only the first entry
		)

		return ret


	@staticmethod
	def get_t0_stats(timestamp):
		"""
		:param timestamp: unix timestamp
		:type timestamp: int, float
		"""

		ret = QueryEventsCol.get(
			tier=0, 
			timestamp=timestamp,
			days_back=int((time() - timestamp) / 86400) + 1
		)

		ret.append(
			{
                "$group": {
                    "_id": 1,
                    "alerts": {
                        "$sum": "$events.metrics.count.alerts"
                    }
                }
            }
		)

		return ret


	@staticmethod
	def get(tier=0, event_name=None, days_back=10, timestamp=None):
		"""
		:param int tier: positive integer between 0 and 3
		:param str event_name: Name of event (job id or task name)
		:param int days_back: positive integer
		:param timestamp: unix time
		:type timestamp: int, float
		"""

		# Array returned by this method
		ret = []

		# restrict match criteria 
		if days_back is not None:

			# Matching db doc ids. Example: [20180711, 20180710]
			match = []

			# add today. Example: 20180711
			match.append(
				int(strftime('%Y%m%d'))
			)

			# add docs from previous days. Example: 20180710, 20180709
			for i in range(1, days_back+1):
				match.append(
					int(
						strftime('%Y%m%d', gmtime(time() - 86400 * i))
					)
				)

			ret.append(
				{
					'$match': {
						'_id': {
							'$in': match
						}
					}
				}
			)

		second_match_stage = {'events.tier': tier}

		if event_name is not None:
			second_match_stage['events.event'] = event_name

		if timestamp is not None:
			second_match_stage['events.dt'] = {'$gt': timestamp}

		ret.extend(
			[
				{'$unwind': '$events'},
				{'$match': second_match_stage},
				# sort events by descending date-time
				{'$sort': {'events.dt': -1}},
			]
		)

		return ret
