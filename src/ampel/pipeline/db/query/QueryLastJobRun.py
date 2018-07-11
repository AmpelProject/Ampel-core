#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/query/QueryLastJobRun.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.07.2018
# Last Modified Date: 11.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime, timedelta

class QueryLastJobRun:
	"""
	"""

	@staticmethod
	def get(job_name, days_back=10):
		"""
		job_name: string
		days_back: positive integer
		"""

		yesterday = int(datetime.strftime(datetime.now() + timedelta(**{'days': -1}), '%Y%m%d'))

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

		ret.extend(
			[
				{'$unwind': '$jobs'},
				{'$match': {'jobs.tier': 3, 'jobs.name': job_name}},
				{'$sort': {'jobs.dt': -1}}, # sort jobs by descending datetime
				{'$limit': 1} # take only the first entry
			]
		)

		return ret
