#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/DBLogPrinter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 25.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime
from ampel.log.LogRecordFlags import LogRecordFlags
from ampel.log.handlers.DBLoggingHandler import DBLoggingHandler

class DBLogPrinter:
	"""
	NEEDS OBERHAUL, NO LONGER WORKING
	"""

	def __init__(self, db):
		"""
		db: instance of pymongo.database.Database
		"""
		self.col = db['jobs']
		self.log_filter_flags = LogRecordFlags(0)


	def set_logging_log_level(self, lvl):
		"""
		Sets required log level ** according to the integer values from the logging module **:
		10: DEBUG, 20: INFO, 30: WARNING, 40: ERROR, 50: CRITICAL

		example: 
			import logging
			import pymongo
			mc = pymongo.MongoClient
			lp = DBLogPrinter(mc)
			lp.set_logging_log_level(logging.INFO) 
		"""
		# pylint: disable=no-member
		self.log_filter_flags |= DBLoggingHandler.severity_map[lvl]


	def set_log_level(self, log_record_flag):
		"""
		set log level filter based on int values from common.flags.LogRecordFlags
		"""
		self.log_filter_flags |= log_record_flag


	def get_logs_with_tranId(self, tran_id):

		#cursor = self.col.find({'records.alertid': alertid})
		lres = list(
			self.col.aggregate(
				[ 
					{"$match": {"records.tranId": tran_id}}, 
					{
						"$project": {
							"records": {
								"$filter": {
									"input": "$records",
									"as": "records",
									"cond": {
										"$eq": ['$$records.tranId', tran_id]
									}
								}
							}
						}
					}
				]
			)
		)

		if len(lres) == 0:
			# pylint: disable=bad-builtin
			print("No log entry matches this criteria")
			return 

		for jobevent in lres:
			for rec in jobevent['records']:
				# pylint: disable=bad-builtin
				print(
					datetime.utcfromtimestamp(
						int(rec['date'])
					).strftime('%Y-%m-%d %H:%M:%S') + " " + rec['msg']
				)
