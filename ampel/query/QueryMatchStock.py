#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/query/QueryMatchStock.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Union, Any

from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.time.QueryTimeModel import QueryTimeModel
from ampel.config.LogicSchemaUtils import LogicSchemaUtils
from ampel.query.QueryGeneralMatch import QueryGeneralMatch


class QueryMatchStock:
	"""
	Use this class to build queries against the ampel "stock" collection
	"""

	@classmethod
	def build_query(cls,
		channels: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		with_tags: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		without_tags: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		time_created: Optional[QueryTimeModel] = None,
		time_modified: Optional[QueryTimeModel] = None
	) -> Dict[str, Any]:
		"""
		:param channels: string (one channel only) or a dict schema \
			(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` \
			for syntax details). None (no criterium) means all channels are considered. 

		:param with_tags: "tags" to be matched by query \
			(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` syntax details). \

		:param without_tags: \
			"tags" not to be matched by query

		:param time_created: \
			match against the (channel dependant) transient document creation timestamp \
			(embedded in the transient journal)

		:param time_modified: \
			match against the (channel dependant) transient document modification timestamp \
			(embedded in the transient journal)

		:returns: query dict instance with adequate matching criteria
		:raises ValueError: QueryGeneralMatch.build can raise ValueError \
			in case the provided dict schema structure is unsupported
		"""

		query = QueryGeneralMatch.build(
			channels=channels,
			with_tags=with_tags,
			without_tags=without_tags
		)

		if time_created or time_modified:
				
			chans = LogicSchemaUtils.reduce_to_set(
				"Any" if channels is None else channels
			)

			or_list = []

			for chan_name in chans:

				and_list = []

				if time_created:
					and_list.append({
						f'created.{chan_name}': time_created.dict()
					})

				if time_modified:
					and_list.append({
						f'modified.{chan_name}': time_modified.dict()
					})

				or_list.append({'$and': and_list})

			query['$or'] = or_list

		return query
