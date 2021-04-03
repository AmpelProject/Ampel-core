#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/var/events.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.07.2018
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import ObjectId
from pymongo.collection import Collection
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Literal, Any, Union, overload


def build_query(
	tier: Literal[0, 1, 2, 3] = 0,
	process_name: Optional[str] = None,
	lte_time: Optional[Union[dict, int, float]] = None,
	gte_time: Optional[Union[dict, int, float]] = None
) -> Dict[str, Any]:
	"""
	:param gte_time: unix timestamp or timedelta argument
	:param lte_time: unix timestamp or timedelta argument
	:returns: list of dict to be used as aggregation pipeline query parameters
	"""

	match: Dict[str, Any] = {}

	if gte_time:
		match['_id'] = {
			"$gte": ObjectId.from_datetime(_get_datetime(gte_time))
		}

	if lte_time:
		if '_id' not in match:
			match['_id'] = {}
		match['_id']['$lte'] = ObjectId.from_datetime(_get_datetime(lte_time))

	if tier:
		match['tier'] = tier

	if process_name:
		match['process'] = process_name

	return match


@overload
def get_last_run(
	col: Collection, process_name: str, require_success: bool,
	gte_time: Optional[Union[dict, float]], timestamp: Literal[True]
) -> Optional[Union[float]]:
	...

@overload
def get_last_run(
	col: Collection, process_name: str, require_success: bool,
	gte_time: Optional[Union[dict, float]], timestamp: Literal[False]
) -> Optional[Union[ObjectId]]:
	...

def get_last_run(
	col: Collection, process_name: str,
	require_success: bool,
	gte_time: Optional[Union[dict, float]] = None,
	timestamp: bool = True
) -> Optional[Union[float, ObjectId]]:
	"""
	:param gte_time: unix timestamp or timedelta argument
	"""

	query = build_query(tier=3, process_name=process_name, gte_time=gte_time)
	if require_success:
		query['success'] = True
	if ret := list(col.find(query).sort('_id', -1).limit(2)):
		if require_success and ret:
			return ret[0]['_id'].generation_time.timestamp() if timestamp else ret[0]['_id']
		elif len(ret) > 1:
			return ret[1]['_id'].generation_time.timestamp() if ret[1]['_id'] else ret[1]['_id']
	return None


def build_t0_stats_query(
	gte_time: Optional[Union[dict, int, float]] = None,
	lte_time: Optional[Union[dict, int, float]] = None
) -> List[Dict]:
	"""
	:param gte_time: unix timestamp or timedelta argument
	:param lte_time: unix timestamp or timedelta argument
	"""

	ret = build_query(tier=0, gte_time=gte_time, lte_time=lte_time)
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


def _get_datetime(t: Union[int, float, dict]) -> datetime:
	if isinstance(t, (int, float)):
		return datetime.fromtimestamp(t)
	elif isinstance(t, dict):
		return datetime.today() + timedelta(**t)
	return None
