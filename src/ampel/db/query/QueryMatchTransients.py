#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/query/QueryMatchTransients.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 24.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.objectid import ObjectId
from ampel.core.flags.AlDocType import AlDocType
from ampel.config.t3.LogicSchemaUtils import LogicSchemaUtils
from ampel.t3.TimeConstraint import TimeConstraint
from ampel.db.query.QueryMatchSchema import QueryMatchSchema


class QueryMatchTransients:
	"""
	"""

	@classmethod
	def match_transients(cls,
		channels=None, with_tags=None, without_tags=None, time_created=None, time_modified=None
	):
		"""
		Merely a shortcut method made of several function calls

		:type channels: str, dict
		:param channels: string (one channel only) or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.pipeline.db.query.QueryMatchSchema>` \
		for syntax details). None (no criterium) means all channels are considered. 

		:type with_tags: str, int, dict
		:param with_tags: string/int (one flag only) or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.pipeline.db.query.QueryMatchSchema>` \
		for syntax details). Important: dict schema must contain **db flags** \
		(integers representing enum members position within enum class), please see \
		:func:`FlagUtils.hash_schema <ampel.core.flags.FlagUtils.hash_schema>` \
		docstring for more info.

		:type without_tags: str, int, dict
		:param without_tags: similar to parameter with_tags, except it's without.

		:param TimeConstraint time_created: instance of ampel.pipeline.t3.TimeConstraint
		:param TimeConstraint time_modified: instance of ampel.pipeline.t3.TimeConstraint

		:rtype: dict
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

		created_constraint = cls._add_time_constraint(time_created)
		modified_constraint = cls._add_time_constraint(time_modified)
		if created_constraint or modified_constraint:
				
			chans = LogicSchemaUtils.reduce_to_set("Any" if channels is None else channels)

			or_list = []
			for chan_name in chans:
				and_list = []
				if created_constraint:
					and_list.append({
						'created.%s' % chan_name: created_constraint
					})

				if modified_constraint:
					and_list.append({
						'modified.%s' % chan_name: modified_constraint
					})
				or_list.append({'$and': and_list})
			query['$or'] = or_list

		return query


	@staticmethod
	def _add_time_constraint(tc):
		"""
		:param TimeConstrain tc: instance of ampel.pipeline.t3.TimeConstraint.py
		:returns: dict such as:
			{
				'$gt': 1223142,
				'$lt': 9894324923
			}
		"""

		if tc is None:
			return None
		if type(tc) is not TimeConstraint:
			raise ValueError("Parameter must be a TimeConstraint instance")

		d = {}
		if tc.has_constraint():
			for key, op in {'after': '$gte', 'before': '$lte'}.items():
				val = tc.get(key)
				if val:
					d[op] = val.timestamp()
		if len(d) == 0:
			return None
		else:
			return d
