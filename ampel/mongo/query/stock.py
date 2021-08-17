#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/stock.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Union, Any, Literal

from ampel.types import Tag
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.time.QueryTimeModel import QueryTimeModel
from ampel.util.logicschema import reduce_to_set
from ampel.mongo.query.general import build_general_query


def build_stock_query(
	channel: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
	tag: Optional[Dict[Literal['with', 'without'], Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]]] = None,
	time_created: Optional[QueryTimeModel] = None,
	time_updated: Optional[QueryTimeModel] = None
) -> Dict[str, Any]:
	"""
	:param channel: string (one channel only) or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` \
		for syntax details). None (no criterium) means all channel are considered.

	:param with_tags: "tags" to be matched by query \
		(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` syntax details). \

	:param without_tags: \
		"tags" not to be matched by query

	:param time_created: \
		match against the (channel dependant) transient document creation timestamp \
		(embedded in the transient journal)

	:param time_updated: \
		match against the (channel dependant) transient document modification timestamp \
		(embedded in the transient journal)

	:returns: query dict instance with adequate matching criteria
	:raises ValueError: build_general_query can raise ValueError \
		in case the provided dict schema structure is unsupported
	"""

	query = build_general_query(channel=channel, tag=tag)

	if time_created or time_updated:

		chans = reduce_to_set("any" if channel is None else channel)
		or_list = []

		for chan_name in chans:

			and_list = []

			if time_created:
				and_list.append({
					f'ts.{chan_name}.tied': time_created.dict()
				})

			if time_updated:
				and_list.append({
					f'ts.{chan_name}.upd': time_updated.dict()
				})

			or_list.append({'$and': and_list})

		query['$or'] = or_list

	return query
