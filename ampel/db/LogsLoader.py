#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/LogsLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.11.2018
# Last Modified Date: 15.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.log.AmpelLogger import AmpelLogger
from ampel.view.ReadOnlyDict import ReadOnlyDict
from ampel.db.AmpelDB import AmpelDB
from ampel.log.LogFlag import LogFlag

# TODO: update this class
class LogsLoader:
	"""
	"""

	def __init__(self, read_only=True, logger=None):
		"""
		"""
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.read_only = read_only


	def fetch_main_logs(
		self, matcher, decompactify=True, simplify=False, hexify=True, resolve_flag=True,
		remove=["channel"], datetime='string', datetime_key='_id', verbose=False
	):
		"""
		:param matcher: insance of LogsMatcher
		:type matcher: LogsMatcher
		:param bool decompactify:
		:param bool resolve_flag: \
			prints 'flag': <LogFlag.SCHEDULED_RUN|CORE|T2|INFO: 8836> \
			instead of 'flag': 8836
		:param bool simplify:
		:param datetime: if not None, datetime of each log entry (extracted from ObjectId) \
			will be added to the log content (use param datetime_key to configure whished key). \
			Possible values: \
			'string': datetime will be added as string: '2019-03-07T08:32:17.000Z' \
			'date': datetime will be added as datetime object: datetime.datetime(2019, 3, 7, 8, 32, 30)
		:param str datetime_key: \
			'_id': garanteed in first position but overrides default _id (don't use remove=["_id"]...) \
			or any string ('date' for example)
		:param bool verbose: whether to print the generated aggregation stages
		:param List[str] remove: possible values: 'channel', '_id', 'tranId'
		"""

		stages = [
			# Matching criteria (can contain nested dicts in case of complex criteria)
			{'$match': matcher.get_match_criteria()}
		]

		# Extract datetime from objectid and add it as 'date' field
		if datetime:
			stages.append(
				{
					'$addFields': {
						datetime_key: {
							"$convert": {
								'input': {'$toDate': '$_id'},
								'to': datetime
							}
						}
					}
				}
			)

		# Unwind, i.e converts
		#  {'_id': ObjectId('5c80d71154048002ca372208'),
		#  'flag': 8740,
		#  'run': 714,
		#  'channel': 3,
		#  'msg': ['msg1', 'msg2'],
		# Into:
		#  {'_id': ObjectId('5c80d71154048002ca372208'),
		#  'flag': 8740,
		#  'run': 714,
		#  'channel': 3,
		#  'msg': 'msg1'},
		#  {'_id': ObjectId('5c80d71154048002ca372208'),
		#  'flag': 8740,
		#  'run': 714,
		#  'channel': 3,
		#  'msg': 'msg2'},
		stages.append(
			{
				'$unwind': {
					'path': '$msg',
					'preserveNullAndEmptyArrays': True
				}
			}
		)

		if decompactify:

			# Converts
			#  {'_id': ObjectId('5c80d71154048002ca372208'),
			#  'stock': 1810112413252531,
			#  'alert': 697252385515010002,
			#  'flag': 8740,
			#  'run': 714,
			#  'msg': {'channel': 3, 'txt': 'test2'},
			#  'date': datetime.datetime(2019, 3, 7, 8, 32, 17)}
			# Into:
			#  {'_id': ObjectId('5c80d71154048002ca372208'),
			#  'stock': 1810112413252531,
			#  'alert': 697252385515010002,
			#  'flag': 8740,
			#  'run': 714,
			#  'msg': 'test2',
			#  'date': datetime.datetime(2019, 3, 7, 8, 32, 17),
			#  'channel': 3}
			#######################################################

			# Part 1
			# a) Copy channel info embedded in compact msg to log entry root level
			# b) If msg.txt exists, copy it to root level as 'msg'
			stages.append(
				{
					"$addFields": {
						"channel": {
							"$ifNull": ["$channel", "$msg.channel"]
						},
						"msg": {
							"$ifNull": [
								"$msg.txt",
								{"$ifNull": ["$msg", "$REMOVE"]}
							]
						},
					}
				}
			)

			# Part 2:
			# remove compact dict leftover
			# ('msg': {'channel': 3, 'txt': 'test2'})
			stages.append(
				{
					"$addFields": {
						"msg": {
							"$cond": {
								"if": {"$not": "$msg.channel"},
								"then": "$msg",
								"else": "$REMOVE"
							}
						}
					}
				}
			)

			# re-apply channel filter as $unwind may have included
			# log entries associated with other channel
			if hasattr(matcher, 'channel'):
				stages.append(
					{
						'$match': {
							'channel': matcher.channels
						}
					}
				)

		if remove:

			# Projection (last aggregation stage)
			proj = {'$project': {}}

			for el in remove:
				proj['$project'][el] = 0

			stages.append(proj)

		if verbose:
			print("Using following aggregation stages:")
			print(stages)

		log_entries = tuple(
			el for el in AmpelDB.get_collection("logs").aggregate(stages)
		)

		if simplify:
			for el in log_entries:
				print("%s %s" % (el['_id'], el['msg']))
			return

		if hexify and resolve_flag:

			for el in log_entries:
				el['flag'] = LogFlag(el['flag'])
				if 'comp' in el:
					el['comp'] = el['comp'].hex()

		else:

			if hexify:
				for el in log_entries:
					if 'comp' in el:
						el['comp'] = el['comp'].hex()

			if resolve_flag:
				for el in log_entries:
					el['flag'] = LogFlag(el['flag'])

		if self.read_only:
			return tuple(ReadOnlyDict(el) for el in log_entries)

		return log_entries
