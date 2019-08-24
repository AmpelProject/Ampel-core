#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/query/QueryLoadTransientInfo.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.02.2018
# Last Modified Date: 12.05.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.binary import Binary
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils
from ampel.common.AmpelUtils import AmpelUtils
from ampel.db.query.QueryMatchSchema import QueryMatchSchema
from ampel.db.query.QueryUtils import QueryUtils

class QueryLoadTransientInfo:
	"""
	"""

	@classmethod
	def build_stateless_query(cls, tran_ids, docs, channels=None, t2_subsel=None):
		"""
		| Builds a pymongo query aiming at loading transient docs (t2s, coumpounds)\
		(not including photpoints/upper limits/transient doc which are in a separate collection). 
		| Stateless query: all avail compounds and t2docs (although possibly \
		constrained by parameter t2_subsel) are targeted.

		:type tran_id: int, List[int], set[int]
		:param tran_id: transient id(s) (int, list of ints, set of ints). \
		Query is generated to be performed on multiple ids at once.

		:type channels: str, dict
		:param channels: string (one channel only) or a dict \
		(see :obj:`QueryMatchSchema <ampel.pipeline.db.query.QueryMatchSchema>` \
		for syntax details). None (no criterium) means all channels are considered. 

		:param List[AlDocType] docs: list of AlDocType enum members. \
		AlDocType.PHOTOPOINT and AlDocType.UPPERLIMIT will be ignored (separate collection).

		:type t2_subsel: str or List[str]
		:param t2_subsel: optional sub-selection of t2 results based on t2 unit id(s). \
		t2_subsel will *include* the provided results of t2s with the given ids \
		and thus exclude other t2 results. \
		If None or an empty list: all t2 docs associated with the matched transients will be loaded. \
		A single t2 unit id (string) or a list of t2 unit ids can be provided.

		:returns: pymongo query matching criteria in form of a dict
		:rtype: dict
		"""

		query = cls.create_broad_query(tran_ids, channels)

		if AlDocType.COMPOUND in docs:
			
			if t2_subsel:
				query['$or'] = [
					{'alDocType': AlDocType.COMPOUND},
					{
						'alDocType': AlDocType.T2RECORD,
						't2UnitId': \
							t2_subsel if type(t2_subsel) is str \
							else QueryUtils.match_array(t2_subsel)
					}
				]

			elif AlDocType.T2RECORD in docs:
				query['alDocType'] = {
					'$in': [AlDocType.COMPOUND, AlDocType.T2RECORD]
				}
			else:
				query['alDocType'] = AlDocType.COMPOUND

		else:

			if AlDocType.T2RECORD in docs or t2_subsel:
				query['alDocType'] = AlDocType.T2RECORD
				if t2_subsel:
					query['t2UnitId'] = \
						t2_subsel if type(t2_subsel) is str \
						else QueryUtils.match_array(t2_subsel)

		# return query matching criteria
		return query


	@classmethod
	def build_statebound_query(
		cls, tran_ids, docs, states, channels=None, t2_subsel=None, comp_already_loaded=False
	):
		"""
		See :func:`build_stateless_query <build_stateless_query>` docstring
		"""

		query = cls.create_broad_query(tran_ids, channels)

		# Check if correct state(s) was provided
		type_state = type(states)

		# Single state was provided as string
		if type_state is str:
			if len(states) != 32:
				raise ValueError("Provided state string must have 32 characters")
			match_comp_ids = Binary(bytes.fromhex(states), 5) # convert to bson Binary

		# Single state was provided as bytes
		elif type_state is bytes:
			if len(states) != 16:
				raise ValueError("Provided state bytes must have a length of 16")
			match_comp_ids = Binary(states, 5) # convert to bson Binary

		# Single state was provided as bson Binary
		elif type_state is Binary:
			if states.subtype != 5:
				raise ValueError("Provided bson Binary state must have subtype 5")
			match_comp_ids = states

		# Multiple states were provided
		elif type_state in (list, tuple, set):

			# check_seq_inner_type makes sure the sequence is monotype
			if not AmpelUtils.check_seq_inner_type(states, (str, bytes, Binary)):
				raise ValueError("Sequence of state must contain element with type: bytes or str")

			first_state = next(iter(states))

			# multiple states were provided as string
			if type(first_state) is str:
				if not all(len(st) == 32 for st in states):
					raise ValueError("Provided state strings must have 32 characters")
				match_comp_ids = {
					'$in': [Binary(bytes.fromhex(st), 5) for st in states] # convert to bson Binary
				}

			# multiple states were provided as bytes
			elif type(first_state) is bytes:
				if not all(len(st) == 16 for st in states):
					raise ValueError("Provided state bytes must have a length of 16")
				match_comp_ids = {
					'$in': [Binary(st, 5) for st in states] # convert to bson Binary
				}

			# multiple states were provided as bson Binary objects
			elif type(first_state) is Binary:
				if not all(st.subtype == 5 for st in states):
					raise ValueError("Bson Binary states must have subtype 5")
				match_comp_ids = {'$in': states if type(states) is list else list(states)}
		else:
			raise ValueError(
				"Type of provided state (%s) must be bytes, str, or sequences of these " % type(states)
			)

		# build query with 'or' connected search criteria
		or_list = []

		if AlDocType.COMPOUND in docs and comp_already_loaded is False:

			or_list.append(
				{
					# no need to specify alDocType: AlDocType.COMPOUND
					'_id': match_comp_ids	
				}
			)

		if AlDocType.T2RECORD in docs:

			t2_match = {
				'alDocType': AlDocType.T2RECORD, 
				'docId': match_comp_ids
			}

			if t2_subsel:
				t2_match['t2UnitId'] = \
					t2_subsel if type(t2_subsel) is str \
					else QueryUtils.match_array(t2_subsel)

			or_list.append(t2_match)

		# If only 1 '$or' criteria was generated, then 
		# just add this criteria to the root dict ('and' connected with tranId: ...)
		if len(or_list) == 1:
			el = or_list[0]
			for key in el.keys():
				query[key] = el[key]
		else:
			query['$or'] = or_list

		return query


	@staticmethod
	def create_broad_query(tran_ids, channels):
		"""
		Creates a broad matching query

		:type tran_id: int, list(int), set(int)
		:param tran_id: transient id(s) (int, list of ints, set of ints). \

		:param channels: see :func:`build_stateless_query <build_stateless_query>` docstring

		:returns: updated match dict
		:rtype: dict
		"""

		query = {
			'tranId': tran_ids if type(tran_ids) is int 
			else QueryUtils.match_array(tran_ids)
		}

		if channels is not None:
			QueryMatchSchema.apply_schema(query, 'channels', channels)

		return query
