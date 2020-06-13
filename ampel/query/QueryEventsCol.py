#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/query/QueryEventsCol.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.07.2018
# Last Modified Date: 10.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import ObjectId
from pymongo.collection import Collection
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Literal, Any, Union


class QueryEventsCol:


	@staticmethod
	def get_last_run(
		col: Collection, process_name: str,
		gte_time: Optional[Union[dict, float]] = None
	) -> Optional[float]:

		query = QueryEventsCol.get(tier=3, process_name=process_name, gte_time=gte_time)
		if ret := list(col.find(query).sort('_id', -1).limit(2)):
			if len(ret) > 1:
				return ret[1]['_id'].generation_time.timestamp()
		return None


	@staticmethod
	def get_t0_stats(
		gte_time: Optional[Union[dict, int, float]] = None,
		lte_time: Optional[Union[dict, int, float]] = None
	) -> List[Dict]:
		"""
		:param gte_time: unix timestamp or timedelta argument
		:param lte_time: unix timestamp or timedelta argument
		"""

		ret = QueryEventsCol.get(tier=0, gte_time=gte_time, lte_time=lte_time)
		ret['metrics.count.alerts'] = {"$exists": True}

		return [
			{'$match': ret},
			{
				"$group": {
					"_id": 1,
					"alerts": {
						"$sum": "$metrics.count.alerts"
					}
				}
			}
		]


	@staticmethod
	def get(
		tier: Literal[0, 1, 2, 3] = 0,
		process_name: Optional[str] = None,
		lte_time: Optional[Union[dict, int, float]] = None,
		gte_time: Optional[Union[dict, int, float]] = None
	) -> Dict[str, Any]:
		"""
		:param tier: positive integer between 0 and 3
		:param days_back: positive integer or None
		:param timestamp: unix time
		:returns: list of dict to be used as aggregation pipeline query parameters
		"""

		match: Dict[str, Any] = {}

		if gte_time:
			match['_id'] = {
				"$gte": ObjectId.from_datetime(
					QueryEventsCol.get_datetime(gte_time)
				)
			}

		if lte_time:
			if '_id' not in match:
				match['_id'] = {}
			match['_id']['$lte'] = ObjectId.from_datetime(
				QueryEventsCol.get_datetime(lte_time)
			)

		if tier:
			match['tier'] = tier

		if process_name:
			match['process'] = process_name

		return match


	@staticmethod
	def get_datetime(t: Union[int, float, dict]) -> datetime:
		if isinstance(t, (int, float)):
			return datetime.fromtimestamp(t)
		elif isinstance(t, dict):
			return datetime.today() + timedelta(**t)
		return None
