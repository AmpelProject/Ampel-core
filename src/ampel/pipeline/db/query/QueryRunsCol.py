#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/query/QueryLastJobRun.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.07.2018
# Last Modified Date: 11.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime, timedelta

class QueryRunsCol:
	"""
	"""

	@staticmethod
	def get_job_last_run(job_name, days_back=10):
		"""
		"""

		ret = QueryRunsCol.get(
			tier=3, job_name=job_name, days_back=days_back
		)

		ret.append(
			{'$limit': 1} # take only the first entry
		)

		return ret


	@staticmethod
	def get_t0_stats(dt):
		"""
		"""

		ret = QueryRunsCol.get(
			tier=0, dt=dt,
			days_back=(datetime.now()-datetime.fromtimestamp(dt)).days + 1
		)

		ret.append(
			{
                "$group": {
                    "_id": 1,
                    "alerts": {
                        "$sum": "$jobs.metrics.count.alerts"
                    }
                }
            }
		)

		return ret


	@staticmethod
	def get(tier=0, job_name=None, days_back=10, dt=None):
		"""
		tier: positive integer between 0 and 3
		job_name: string
		days_back: positive integer
		dt: unix time
		"""

		# Array returned by this method
		ret = []

		# restrict match criteria 
		if days_back is not None:

			# Matching db doc ids. Example: [20180711, 20180710]
			match = []

			# add today. Example: 20180711
			match.append(
				int(datetime.today().strftime('%Y%m%d'))
			)

			# add docs from previous days. Example: 20180710, 20180709
			for i in range(1, days_back+1):
				match.append(
					int(
						datetime.strftime(
							datetime.now() + timedelta(**{'days': -i}), 
							'%Y%m%d'
						)
					)
				)

			ret.append(
				{'$match': {'_id': {'$in': match}}}
			)

		second_match_stage = {'jobs.tier': tier}

		if job_name is not None:
			second_match_stage['jobs.name'] = job_name

		if dt is not None:
			second_match_stage['jobs.dt'] = {'$gt': dt}

		ret.extend(
			[
				{'$unwind': '$jobs'},
				{'$match': second_match_stage},
				{'$sort': {'jobs.dt': -1}}, # sort jobs by descending datetime
			]
		)

		return ret
