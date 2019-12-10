#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/query/QueryMatchStock.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Union

from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.time.QueryTimeModel import QueryTimeModel
from ampel.config.LogicSchemaUtils import LogicSchemaUtils
from ampel.query.QueryMatchSchema import QueryMatchSchema


class QueryMatchStock:
	"""
	Builds a dict to be used as parameter in a mongoDB collection query.
	"""

	@classmethod
	def build_query(cls,
		channels: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		with_tags: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		without_tags: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		time_created: Optional[QueryTimeModel] = None,
		time_modified: Optional[QueryTimeModel] = None
	) -> Dict:
		"""
		:param channels: string (one channel only) or a dict schema \
			(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` \
			for syntax details). None (no criterium) means all channels are considered. 
		:param with_tags: "tags" to be matched by query \
			(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` syntax details). \
		:param without_tags: "tags" not to be matched by query
		:returns: query dict with matching criteria
		"""

		query = {}

		if channels is not None:
			QueryMatchSchema.apply_schema(
				query, 'channels', channels
			)

		if with_tags is not None:
			QueryMatchSchema.apply_schema(
				query, 'alTags', with_tags
			)

		# Order matters, parse_dict(...) must be called *after* parse_excl_dict(...)
		if without_tags is not None:
			QueryMatchSchema.apply_excl_schema(
				query, 'alTags', without_tags
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
