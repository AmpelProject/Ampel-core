#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/dbquery/LoadTransientQuery.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.02.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils


class LoadTransientQuery:
	"""
	"""

	@staticmethod
	def load_transient_query(tran_id, content_types, t2_ids=None):
		"""
		Please note that if AlDocTypes.COMPOUND is included in content_types, 
		document with AlDocTypes.PHOTOPOINT will be queried as well since photopoints instances
		are required to instanciate LightCurve objects.

		Stateless query: all compounds and (possible t2_ids limited) t2docs to be retrieved	
		"""

		query = {'tranId': tran_id}

		# Everything should be retrieved (AlDocTypes: 1+2+4+8=15)
		if t2_ids is None and content_types.value == 15:
			return query

		# Build array of AlDocTypes (AlDocTypes.T2RECORD will be set later since it depends in t2_ids)
		al_types = []
		for al_type in [AlDocTypes.PHOTOPOINT, AlDocTypes.COMPOUND, AlDocTypes.TRANSIENT]:
			if al_type in content_types:
				al_types.append(al_type)

		# Loading LightCurve instances requires photopoints information
		if AlDocTypes.COMPOUND in content_types and AlDocTypes.PHOTOPOINT not in content_types:
			al_types.append(AlDocTypes.PHOTOPOINT)

		if t2_ids is None:

			# Complete alDocType with type T2RECORD if so whished
			if AlDocTypes.T2RECORD in content_types:
				al_types.append(AlDocTypes.T2RECORD)

			# Add single additional search criterium to query
			query['alDocType'] = (
				al_types[0] if len(al_types) == 1 
				else {'$in': al_types}
			)

		else:

			# Convert provided T2UnitIds into an array on flag values
			t2_ids_db = FlagUtils.enumflag_to_dbflag(t2_ids)

			# Combine first part of query (photopoints+transient+compounds) with 
			# runnableId targeted T2RECORD query
			query['$or'] = [
				# photopoints+transient+compounds 
				{
					'alDocType': (
						al_types[0] if len(al_types) == 1 
						else {'$in': al_types}
					)
				},
				{
					'alDocType': AlDocTypes.T2RECORD,
					't2Unit': (
						t2_ids_db[0] if len(t2_ids_db) == 1 
						else {'$in': t2_ids_db}
					)
				}
			]

		return query


	@staticmethod
	def load_transient_state_query(
		tran_id, content_types, compound_id, t2_ids=None, comp_already_loaded=False
	):
		"""
		Please note that if AlDocTypes.COMPOUND is included in content_types, 
		document with AlDocTypes.PHOTOPOINT will be queried as well since photopoints instances
		are required to instanciate LightCurve objects.
		"""

		# Logic check
		if AlDocTypes.COMPOUND not in content_types and AlDocTypes.T2RECORD not in content_types :
			raise ValueError(
				"State scoped queries make no sense without either AlDocTypes.COMPOUND " +
				"or AlDocTypes.T2RECORD set in content_types"
			)

		query = {'tranId': tran_id}

		# build query with 'or' connected search criteria
		or_list = []

		if (
			AlDocTypes.TRANSIENT|AlDocTypes.PHOTOPOINT in content_types or
			AlDocTypes.TRANSIENT|AlDocTypes.COMPOUND in content_types
		):

			or_list.append(
				{
					'alDocType': {
						'$in': [
							AlDocTypes.TRANSIENT, 
							AlDocTypes.PHOTOPOINT
						]
					}
				}
			)

		else:

			if AlDocTypes.TRANSIENT in content_types:
				or_list.append(
					{'alDocType': AlDocTypes.TRANSIENT}
				)

			if (
				AlDocTypes.PHOTOPOINT in content_types or 
				AlDocTypes.COMPOUND in content_types
			):
				or_list.append(
					{'alDocType': AlDocTypes.PHOTOPOINT}
				)

		if AlDocTypes.COMPOUND in content_types and comp_already_loaded is False:

			if type(compound_id) is list:
				match_val = compound_id[0] if len(compound_id) == 1 else {'$in': compound_id}
			else:
				match_val = compound_id

			or_list.append(
				{
					'alDocType': AlDocTypes.COMPOUND, 
					'_id': match_val
				}
			)

		if AlDocTypes.T2RECORD in content_types:

			if type(compound_id) is list:
				match_val = compound_id[0] if len(compound_id) == 1 else {'$in': compound_id}
			else:
				match_val = compound_id
			
			rec_dict = {
				'alDocType': AlDocTypes.T2RECORD, 
				'compoundId': match_val
			}

			if t2_ids is not None:
				t2_ids_db = FlagUtils.enumflag_to_dbflag(t2_ids)
				rec_dict['t2Runnable'] = (
					t2_ids_db[0] if len(t2_ids_db) == 1 
					else {'$in': t2_ids_db}
				)

			or_list.append(rec_dict)

		# If only 1 $or criteria was generated, then 
		# just add this criteria to the root dict ('and' connected with tranId: ...)
		if len(or_list) == 1:
			el = or_list[0]
			for key in el.keys():
				query[key] = el[key]
		else:
			query['$or'] = or_list

		return query
