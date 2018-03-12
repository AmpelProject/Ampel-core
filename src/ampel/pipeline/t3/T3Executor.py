#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Executor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.flags.TransientFlags import TransientFlags
from ampel.pipeline.db.query.MatchTransientsQuery import MatchTransientsQuery
from datetime import timedelta


class T3Executor:
	"""
	"""

	def __init__(self, db, t3_job, collection="main", logger=None):

		col = db[collection]
		logger = LoggingUtils.get_logger() if logger is None else logger

		# T3 job not requiring any prio transient loading
		if t3_job.tran_sel_options() is None:
			t3_task = t3_job.get_task()
			t3_task.get_t3_unit().run(
				t3_task.get_run_config()
			)
			return

		select_options = t3_job.tran_sel_options()

		# Build query using criteria defined in job_config
		query = MatchTransientsQuery.match_transients(
			time_created={'delta': timedelta(**select_options['timedelta'])},
			channels = select_options['channels'] if 'channels' in select_options else None,
			with_flags = select_options['withFlags'] if 'withFlags' in select_options else None,
			without_flags = select_options['withoutFlags'] if 'withoutFlags' in select_options else None
		)

		# Retrieve ids of matching transients (can become a big list)
		tran_ids = [el['tranId'] for el in col.find(query, {'_id':0, 'tranId':1})]

		chunk_size = t3_job.get_chunk() 

		if chunk_size is None:
			chunk_size = len(tran_ids)

		#for i in range(0, len(tran_ids), chunk_size):

		#if chunk_size is None or len(tran_ids) < chunk_size:
		#	self.do_it(tran_ids)
		#else:
		#	for i in range(0, len(tran_ids), chunk_size):
		#		self.do_it(tran_ids[i:i+chunk_size])
