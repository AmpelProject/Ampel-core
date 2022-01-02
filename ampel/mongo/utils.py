#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/utils.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                31.10.2018
# Last Modified Date:  09.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from collections.abc import Sequence
from ampel.types import StrictIterable, strict_iterable


def add_or(query: dict[str, Any], arg: dict[str, Any]) -> None:

	if '$or' in query:
		if '$and' in query:
			if not isinstance(query['$and'], list):
				raise ValueError(f"Illegal $and value in query: {query}")
			query['$and'].append(arg)
		else:
			query['$and'] = [query.pop('$or'), arg]

	else:
		query['$or'] = arg


def maybe_match_array(arg: StrictIterable):
	"""
	maybe_match_array(['ab']) -> returns 'ab'
	maybe_match_array({'ab'}) -> returns 'ab'
	maybe_match_array(['a', 'b']) -> returns {$in: ['a', 'b']}
	maybe_match_array({'a', 'b'}) -> returns {$in: ['a', 'b']}
	"""

	if not isinstance(arg, strict_iterable):
		raise ValueError(
			f"Provided argument is not sequence ({type(arg)})"
		)

	if len(arg) == 1:
		return next(iter(arg))

	if isinstance(arg, list):
		return {'$in': arg}

	# Otherwise cast to list
	return {'$in': list(arg)}


def maybe_use_each(arg: Sequence[Any]) -> Any:
	if isinstance(arg, dict):
		return arg
	if len(arg) == 1:
		return next(iter(arg))
	return {'$each': arg}


def get_ids(col: Any, *, filter_stage: None | dict = None) -> set[Any]:
	"""
	Note1: timeit perf of find vs aggregate for cols with 10 / 700 elements:
	[el['_id'] for el in col.find()] -> 518 µs / 6.1 ms
	list(col.aggregate([{"$project": ..}, {"$group": ..}])) -> 536 µs / 1.72 ms

	Pagination will be required (using $sort, $skip, $limit) if the returned doc exceeds 16MB
	"""

	agg = [
		{"$project": {"_id": "$_id"}},
		{"$group": {"_id": None, "ids": {"$push": "$$ROOT._id"}}}
 	]

	if filter_stage:
		agg.insert(0, filter_stage)

	if res := next(col.aggregate(agg), None):
		return set(res['ids'])
	return set()
