#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/dbquery/QueryMatchTransients.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from bson.objectid import ObjectId
from datetime import datetime, timedelta

from ampel.pipeline.db.query.QueryMatchFlags import QueryMatchFlags
from ampel.pipeline.db.query.QueryMatchCriteria import QueryMatchCriteria


class QueryMatchTransients:
	"""
	"""

	@staticmethod
	def match_transients(
		with_flags=None, without_flags=None, channels=None, 
		time_created={"delta": None, "from": None, "until": None},
		time_modified={"delta": None, "from": None, "until": None}
	):
		"""
		'with_flags': 
		-> instance of ampel.flags.TransientFlags or list of instances of TransientFlags
		Transient matching the with_flags criteria will be included.
		See QueryMatchFlags.add_match_criteria docstring for more info

		'without_flags': 
		-> instance of ampel.flags.TransientFlags or list of instances of TransientFlags
		Transient matching with the with_flags criteria will be excluded.
		See QueryMatchFlags.add_nomatch_criteria docstring for more info

		'channels': 
		Either:
			-> instance of ampel.flags.ChannelFlags or list of instances of ChannelFlags
			See QueryMatchFlags.add_match_criteria docstring for more info
		Or:
			-> list of strings (can be nested up to one level)
			See QueryMatchCriteria.add_from_list docstring for more info

		'time_created': 
			-> provide either 'delta' or ('from' and/or 'until')
			-> 'delta': instance of datetime.timedelta
			-> 'from' and 'until' must be of type datetime.datetime

		'time_modified': 
			-> provide either 'delta' or ('from' and/or 'until')
			-> 'delta': instance of datetime.timedelta
			-> 'from' and 'until' must be of type datetime.datetime
		"""

		query = {
			'alDocType': AlDocTypes.TRANSIENT
		}

		if with_flags is not None:
			QueryMatchFlags.add_match_criteria(with_flags, query, 'alFlags')

		# Order matters, add_nomatch_criteria(...) must be called *after* add_match_criteria(...)
		if without_flags is not None:
			QueryMatchFlags.add_nomatch_criteria(without_flags, query, 'alFlags')

		if channels is not None:
			QueryMatchCriteria.add_from_list(
				query, 'channels',
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels))
			)

		QueryMatchTransients._build_time_contraint(query, '_id', time_created, is_oid=True)
		QueryMatchTransients._build_time_contraint(query, 'modified', time_modified)

		return query


	@staticmethod
	def _build_time_contraint(query, db_field_name, time_constraint, is_oid=False):
		"""
		"""
		if time_constraint['delta'] is not None:
			gen_time = datetime.today() + time_constraint['delta'] 
			query[db_field_name] = {
				"$gte": ObjectId.from_datetime(gen_time) if is_oid else gen_time
			}

		if (
			('from' in time_constraint and time_constraint['from'] is not None) or 
			('until' in time_constraint and time_constraint['until'] is not None)
		):

			# TODO: check this, I don't get it anymore
			if db_field_name in query:
				raise ValueError(
					"Wrong time_constraint criteria: " +
					"please use either 'delta' or ('from' and/or 'until'))"
				)

			query[db_field_name] = {}

			if time_constraint['from'] is not None:
				query[db_field_name]["$gte"] = (
					ObjectId.from_datetime(time_constraint['from']) if is_oid
					else time_constraint['from']
				)

			if time_constraint['until'] is not None:
				query[db_field_name]["$lte"] = (
					ObjectId.from_datetime(time_constraint['until']) if is_oid
					else time_constraint['until']
				)
