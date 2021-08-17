#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/var/LogsLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.11.2018
# Last Modified Date: 16.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, List, Literal, Dict, Any, Sequence
from pymongo.collection import Collection
from ampel.base.AmpelFlexModel import AmpelFlexModel
from ampel.view.ReadOnlyDict import ReadOnlyDict
from ampel.content.LogDocument import LogDocument
from ampel.log.LogFlag import LogFlag


class LogsLoader(AmpelFlexModel):
	"""
	Abbreviations:
	s: stock, a: alert, f: flag, r: run, m: msg, c: channel

	:param decompactify:
	:param resolve_flag: load flag int as LogFlag
		(repr becomes <LogFlag.SCHEDULED_RUN|CORE|T2|INFO: 8836> instead of 8836)
	:param bool simplify:
	:param datetime_ouput: if not None, datetime of each log entry (extracted from ObjectId)
		will be added to the log content (use param datetime_key to configure whished key).
		Possible values:
		- 'string': datetime will be added as string: '2019-03-07T08:32:17.000Z'
		- 'date': datetime will be added as datetime object: datetime.datetime(2019, 3, 7, 8, 32, 30)
	:param datetime_key: '_id': garanteed in first position but overrides default _id (don't use remove=["_id"]...)
		or any string ('date' for example)
	:param debug: print generated aggregation stages if True
	:param remove_keys: remove key (possible values: 'c', '_id', 's') during projection stage
	"""

	decompactify: bool = True
	simplify: bool = False
	hexify: bool = True
	resolve_flag: bool = True
	read_only: bool = False
	remove_keys: Optional[List[str]] = ["channel"]
	datetime_ouput: Literal['string', 'date'] = 'string'
	datetime_key: str = '_id'
	verbose: bool = False
	debug: bool = False


	def fetch_logs(self,
		col: Collection,
		match: Optional[Dict[str, Any]] = None,
		channel: Optional[Dict[str, Any]] = None
	) -> Sequence[LogDocument]:
		"""
		:param match: match criteria
		:param channel: The $unwind aggregagtion pipeline stage requires to re-apply channel match criteria.
			These are not necessarily fetchable using match.get('channel') since complex match logic can use the $or operator.
			(No need to use this argument if match['channel'] exists, it is used automatically in this case)
		"""

		stages = [
			# Matching criteria (can contain nested dicts in case of complex criteria)
			{'$match': match or {}}
		]

		# Extract datetime from objectid and add it as 'date' field
		if self.datetime_ouput:
			stages.append(
				{
					'$addFields': {
						self.datetime_key: {
							"$convert": {
								'input': {'$toDate': '$_id'},
								'to': self.datetime_ouput
							}
						}
					}
				}
			)

		# Unwind, i.e converts
		#  {'_id': ObjectId('5c80d71154048002ca372208'),
		#  'f': 8740,
		#  'r': 714,
		#  'c': 3,
		#  'm': ['msg1', 'msg2'],
		# Into:
		#  {'_id': ObjectId('5c80d71154048002ca372208'),
		#  'f': 8740,
		#  'r': 714,
		#  'c': 3,
		#  'm': 'msg1'},
		#  {'_id': ObjectId('5c80d71154048002ca372208'),
		#  'f': 8740,
		#  'r': 714,
		#  'c': 3,
		#  'm': 'msg2'},
		stages.append(
			{
				'$unwind': {
					'path': '$m',
					'preserveNullAndEmptyArrays': True
				}
			}
		)

		if self.decompactify:

			# Converts
			#  {'_id': ObjectId('5c80d71154048002ca372208'),
			#  's': 1810112413252531,
			#  'a': 697252385515010002,
			#  'f': 8740,
			#  'r': 714,
			#  'm': {'c': 3, 't': 'test2'},
			#  'date': datetime.datetime(2019, 3, 7, 8, 32, 17)}
			# Into:
			#  {'_id': ObjectId('5c80d71154048002ca372208'),
			#  's': 1810112413252531,
			#  'a': 697252385515010002,
			#  'f': 8740,
			#  'r': 714,
			#  'm': 'test2',
			#  'date': datetime.datetime(2019, 3, 7, 8, 32, 17),
			#  'c': 3}
			#######################################################

			# Part 1
			# a) Copy channel info embedded in compact msg to log entry root level
			# b) If m.m exists, copy it to root level as 'msg'
			stages.append(
				{
					"$addFields": {
						"c": {
							"$ifNull": ["$c", "$m.c"]
						},
						"m": {
							"$ifNull": [
								"$m.m",
								{"$ifNull": ["$m", "$REMOVE"]}
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
						"m": {
							"$cond": {
								"if": {"$not": "$m.c"},
								"then": "$m",
								"else": "$REMOVE"
							}
						}
					}
				}
			)

			# re-apply channel filter as $unwind may have included
			# log entries associated with other channel
			if (match and (c := match.get('channel'))) or channel:
				stages.append(
					{'$match': {'c': c or channel}}
				)

		if self.remove_keys:

			# Projection (last aggregation stage)
			proj: Dict[str, Any] = {'$project': {}}

			for k in self.remove_keys:
				proj['$project'][k] = 0

			stages.append(proj)

		if self.debug:
			print("Using aggregation: %s" % stages)

		log_entries: List[LogDocument] = list(col.aggregate(stages))

		if self.resolve_flag:
			for el in log_entries:
				el["f"] = LogFlag(el["f"])

		if self.simplify:
			for el in log_entries:
				print("%r %s" % (el["_id"], el['m']))
			return []

		# if hexify:
		#	for el in log_entries:
		#		if 'e' in el:
		#			el['e']['comp'] = el['e']['comp'].hex()

		if self.read_only:
			return tuple(ReadOnlyDict(el) for el in log_entries) # type: ignore

		return log_entries
