from ampel.pipeline.common.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.common.db.DBLoggingHandler import DBLoggingHandler

class DBLogPrinter:

	def __init__(self, mongo_client):
		db = mongo_client["events"]
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
		self.log_filter_flags |= DBLoggingHandler.severity_map[lvl]

	def set_log_level(self, log_record_flag):
		"""
			set log level filter based on int values from common.flags.LogRecordFlags
		"""
		self.log_filter_flags |= log_record_flag

	def get_alertid_logs(self, alertid):
		import datetime

		#cursor = self.col.find({'records.alertid': alertid})
		lres = list(self.col.aggregate([ 
			{"$match": {"records.alertid": alertid}}, 
			{"$project": {
				"records": {
					"$filter": {
						"input": "$records",
						"as": "records",
						"cond": {
							"$eq": ['$$records.alertid', alertid]
						}
					}
				}
			}
			}
		]))

		if len(lres) == 0:
			print("No log entry matches this criteria")
			return 

		for jobevent in lres:
			#print(datetime.datetime.fromtimestamp(int(line['date'])).strftime('%Y-%m-%d %H:%M:%S') + line['msg'])
			for rec in jobevent['records']:
				print(datetime.datetime.fromtimestamp(int(rec['date'])).strftime('%Y-%m-%d %H:%M:%S') + " " + rec['msg'])
