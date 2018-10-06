#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/query/QueryLoadTransientInfo.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.02.2018
# Last Modified Date: 02.08.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.binary import Binary
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.db.query.QueryMatchCriteria import QueryMatchCriteria

class QueryLoadTransientInfo:
	"""
	"""

	limited_altypes = (
		AlDocType.COMPOUND,
		AlDocType.TRANSIENT
	)

	@staticmethod
	def build_stateless_query(tran_ids, content, channels=None, t2_subsel=None):
		"""
		Loads transient info (not including photpoints/upper limits which are in a separate collection).
		Stateless query: all compounds and t2docs (although possibly constrained by parameter t2_subsel) 
		will be retrieved.

		:param tran_id: transient id(s) (int, list of ints, set of ints). 
		  Query can be built so that it is performed on multiple ids at once.
		:type tran_id: int, list(int), set(int)

		:param channels: A string, a list of strings or a 2d list of strings:
			- a list of strings (whereby each string represents a channel name)
			- a 2d list of strings 
				* outer list: elements connected by OR
				* innerlist: elements connected by AND
		:type channels: str, list(str)

		:param AlDocType content: instance of AlDocType. 
		AlDocType.PHOTOPOINT and AlDocType.UPPERLIMIT will be ignored (separate collection).
		Note: list are not accepted. AlDocType Enum members are AND connected which one 
		could interpret as one wants to collect "all" provided content (which in the end, 
		said aside, ends up in a search query with search criteria connected by OR).

		:param t2_subsel: optional sub-selection of t2 results based on t2 unit id(s).
		t2_subsel will *include* the provided results of t2s with the given ids
		and thus exclude other t2 results.
		If None or an empty list: all t2 docs associated with the matched transients will be loaded. 
		A single t2 unit id (string) or a list of t2 unit ids can be provided.
		:type t2_subsel: str or list(str)
		"""

		match_dict = QueryLoadTransientInfo.create_broad_match_dict(tran_ids, channels)

		# Everything should be retrieved (AlDocType: 1+2+4+8=15)
		if not t2_subsel and content.value == 15:
			return match_dict

		# Build array of AlDocType (AlDocType.T2RECORD will be set later since it depends in t2_subsel)
		al_types = [
			al_type for al_type in QueryLoadTransientInfo.limited_altypes 
			if al_type in content
		]

		if not t2_subsel:

			# Complete alDocType with type T2RECORD if so wished
			if AlDocType.T2RECORD in content:
				al_types.append(AlDocType.T2RECORD)

			match_dict['alDocType'] = (
				al_types[0] if len(al_types) == 1 
				else {'$in': al_types}
			)

			# return query matching criteria
			return match_dict

		else:

			# Combine first part of query (transient+compounds) 
			# with t2UnitId targeted T2RECORD query
			match_dict['$or'] = [
				# transient+compounds 
				{
					'alDocType': (
						al_types[0] if len(al_types) == 1 
						else {'$in': al_types}
					)
				},
				# t2s
				{
					'alDocType': AlDocType.T2RECORD,
					't2Unit': t2_subsel if type(t2_subsel) is str else (
						t2_subsel[0] if len(t2_subsel) == 1 else {'$in': t2_subsel}
					)
				}
			]

		return match_dict


	@staticmethod
	def build_statebound_query(
		tran_ids, content, states, channels=None, t2_subsel=None, comp_already_loaded=False
	):
		"""
		See build_stateless_query docstring
		"""

		# Logic check
		#if AlDocType.COMPOUND not in content and AlDocType.T2RECORD not in content :
		#	raise ValueError(
		#		"State scoped queries make no sense without either AlDocType.COMPOUND " +
		#		"or AlDocType.T2RECORD set in content"
		#	)

		match_dict = QueryLoadTransientInfo.create_broad_match_dict(tran_ids, channels)

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

		if AlDocType.TRANSIENT in content:

			or_list.append(
				{'alDocType': AlDocType.TRANSIENT}
			)

		if AlDocType.COMPOUND in content and comp_already_loaded is False:

			or_list.append(
				{
					'alDocType': AlDocType.COMPOUND, 
					'_id': match_comp_ids	
				}
			)

		if AlDocType.T2RECORD in content:

			t2_match = {
				'alDocType': AlDocType.T2RECORD, 
				'compId': match_comp_ids
			}

			if t2_subsel:
				t2_match['t2Unit'] = (
					t2_subsel if type(t2_subsel) is str 
					else (
						t2_subsel[0] if len(t2_subsel) == 1 
						else {'$in': t2_subsel}
					)
				)

			or_list.append(t2_match)

		# If only 1 $or criteria was generated, then 
		# just add this criteria to the root dict ('and' connected with tranId: ...)
		if len(or_list) == 1:
			el = or_list[0]
			for key in el.keys():
				match_dict[key] = el[key]
		else:
			match_dict['$or'] = or_list

		return match_dict


	@staticmethod
	def create_broad_match_dict(tran_ids, channels):
		"""
		"""

		match_dict = {}

		match_dict['tranId'] = ( 
			tran_ids if type(tran_ids) is int
			else {'$in': tran_ids if type(tran_ids) is list else list(tran_ids)}
		)

		if channels is not None:
			QueryMatchCriteria.add_from_list(
				match_dict, 'channels',
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels)), 
			)

		return match_dict
