#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/query/QueryMatchTransients.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 21.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from bson.objectid import ObjectId
from datetime import datetime, timedelta

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.pipeline.db.query.QueryMatchFlags import QueryMatchFlags
from ampel.pipeline.db.query.QueryMatchCriteria import QueryMatchCriteria


class QueryMatchTransients:
	"""
	"""

	@staticmethod
	def match_transients(
		channels=None, with_flags=None, without_flags=None, time_created=None, time_modified=None
	):
		"""
		'channels': list of strings (can be nested up to one level)
		** See QueryMatchCriteria.add_from_list docstring for more info **

		'with_flags': 
		-> instance of ampel.flags.TransientFlags or list of instances of TransientFlags
		Transient matching the with_flags criteria will be included.
		See QueryMatchFlags.add_match_criteria docstring for more info

		'without_flags': 
		-> instance of ampel.flags.TransientFlags or list of instances of TransientFlags
		Transient matching with the with_flags criteria will be excluded.
		See QueryMatchFlags.add_nomatch_criteria docstring for more info

		'time_created': instance of ampel.pipeline.t3.TimeConstraint
		'time_modified': instance of ampel.pipeline.t3.TimeConstraint
		"""

		query = {
			'alDocType': AlDocTypes.TRANSIENT
		}

		if with_flags is not None:
			QueryMatchFlags.add_match_criteria(
				with_flags, query, 'alFlags'
			)

		# Order matters, add_nomatch_criteria(...) must be called *after* add_match_criteria(...)
		if without_flags is not None:
			QueryMatchFlags.add_nomatch_criteria(
				without_flags, query, 'alFlags'
			)

		if channels is not None:
			QueryMatchCriteria.add_from_list(
				query, 'channels',
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels))
			)
	
		if time_created is not None:
			if not type(time_created) is TimeConstraint:
				raise ValueError("Parameter 'time_created' must be a TimeConstraint instance")
			QueryMatchTransients._add_time_constraint(
				query, '_id', time_created, oid_match=True
			)

		if time_modified is not None:
			if type(time_modified) is not TimeConstraint:
				raise ValueError("Parameter 'time_modified' must be a TimeConstraint instance")
			QueryMatchTransients._add_time_constraint(
				query, 'modified', time_modified
			)

		return query


	@staticmethod
	def _add_time_constraint(query, target_field, tc_obj, oid_match=False):
		"""
		'query': dict used for the pymongo find() request
		'target_field': typically '_id' or 'modified'
		'tc_obj': instance of ampel/pipeline/t3/TimeConstraint.py
		'oid_match': is the targeted field value an ObjectId or not
		"""
		if tc_obj.has_constraint():
			for key, op in {'from': '$gte', 'until': '$lte'}.items():
				val = tc_obj.get(key)
				if val is not None:
					if target_field not in query:
						query[target_field] = {}
					query[target_field][op] = ObjectId.from_datetime(val) if oid_match else val.timestamp()
