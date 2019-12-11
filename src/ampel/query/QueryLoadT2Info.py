#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/query/QueryLoadT2Info.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.02.2018
# Last Modified Date: 11.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.binary import Binary
from typing import Iterable, Union, Dict, Any
from ampel.typing import StrictIterable
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.common.AmpelUtils import AmpelUtils
from ampel.query.QueryUtils import QueryUtils
from ampel.query.QueryMatchSchema import QueryMatchSchema


class QueryLoadT2Info:
	""" """

	@classmethod
	def build_stateless_query(cls, 
		stock_ids: Union[int, str, Iterable[Union[int, str]]], 
		channels: Union[int, str, Dict, AllOf, AnyOf, OneOf],
		t2_subsel: Union[int, str, Iterable[Union[int, str]]] = None
	) -> Dict[str, Any]:
		"""
		| Builds a pymongo query dict aiming at loading transient t2 or compounds docs \
		| Stateless query: all avail compounds and t2docs (although possibly \
		constrained by parameter t2_subsel) are targeted.

		:param stock_ids: (query can be performed on multiple ids at once)

		:param channels: see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` for details. \
		None (no criterium) means all channels are considered. 

		:param t2_subsel: optional sub-selection of t2 records based on t2 class names. \
		-> only t2 records matching with the provided t2 class names will be returned. \
		If None (or empty iterable): all t2 docs associated with the matched transients will be targeted. \
		"""

		query = cls.create_broad_query(stock_ids, channels)

		if t2_subsel:
			query['t2Id'] = t2_subsel if isinstance(t2_subsel, str) \
				else QueryUtils.match_array(t2_subsel)

		return query


	@classmethod
	def build_statebound_t1_query(cls,
		states: Union[str, bytes, Binary, Iterable[str], Iterable[bytes], Iterable[Binary]],
	) -> Dict[str, Any]:
		"""
		"""
		return {
			'_id': cls.get_compound_match(states)
		}


	@classmethod
	def build_statebound_t2_query(cls, 
		stock_ids: Union[int, str, Iterable[Union[int, str]]], 
		states: Union[str, bytes, Binary, Iterable[str], Iterable[bytes], Iterable[Binary]],
		channels: Union[int, str, Dict, AllOf, AnyOf, OneOf],
		t2_subsel: Union[int, str, Iterable[Union[int, str]]] = None
	) -> Dict[str, Any]:
		"""
		See :func:`build_stateless_query <build_stateless_query>` docstring
		"""

		query = cls.create_broad_query(stock_ids, channels)
		query['docId'] = cls.get_compound_match(states)

		if t2_subsel:
			query['t2Id'] = t2_subsel if isinstance(t2_subsel, str) \
			else QueryUtils.match_array(t2_subsel)

		return query


	@staticmethod
	def get_compound_match(
		states: Union[str, bytes, Binary, Iterable[str], Iterable[bytes], Iterable[Binary]]
	) -> Dict[str, Any]:
		"""
		:raises ValueError: if provided states parameter is invalid
		"""

		# Single state was provided as string
		if isinstance(states, str):
			if len(states) != 32:
				raise ValueError("Provided state string must have 32 characters")
			match_comp_ids = Binary(bytes.fromhex(states), 5) # convert to bson Binary

		# Single state was provided as bytes
		elif isinstance(states, bytes):
			if len(states) != 16:
				raise ValueError("Provided state bytes must have a length of 16")
			match_comp_ids = Binary(states, 5) # convert to bson Binary

		# Single state was provided as bson Binary
		elif isinstance(states, Binary):
			if states.subtype != 5:
				raise ValueError("Provided bson Binary state must have subtype 5")
			match_comp_ids = states

		# Multiple states were provided
		elif isinstance(states, StrictIterable):

			# check_seq_inner_type makes sure the sequence is monotype
			if not AmpelUtils.check_seq_inner_type(states, (str, bytes, Binary)):
				raise ValueError("Sequence of state must contain element with type: bytes or str")

			first_state = next(iter(states))

			# multiple states were provided as string
			if isinstance(first_state, str):
				if not all(len(st) == 32 for st in states):
					raise ValueError("Provided state strings must have 32 characters")
				match_comp_ids = {
					'$in': [Binary(bytes.fromhex(st), 5) for st in states] # convert to bson Binary
				}

			# multiple states were provided as bytes
			elif isinstance(first_state, bytes):
				if not all(len(st) == 16 for st in states):
					raise ValueError("Provided state bytes must have a length of 16")
				match_comp_ids = {
					'$in': [Binary(st, 5) for st in states] # convert to bson Binary
				}

			# multiple states were provided as bson Binary objects
			elif isinstance(first_state, Binary):
				if not all(st.subtype == 5 for st in states):
					raise ValueError("Bson Binary states must have subtype 5")
				match_comp_ids = {'$in': states if isinstance(states, list) else list(states)}

		else:
			raise ValueError(
				f"Type of provided state ({type(states)}) must be "
				f"bytes, str, bson.Binary or sequences of these"
			)

		return match_comp_ids


	@staticmethod
	def create_broad_query(
		stock_ids: Union[int, str, Iterable[Union[int, str]]], 
		channels: Union[int, str, Dict, AllOf, AnyOf, OneOf]
	) -> Dict[str, Any]:
		"""
		:param channels: see :func:`build_stateless_query <build_stateless_query>` docstring
		"""

		query = {
			'stockId': QueryUtils.match_array(stock_ids) \
				if isinstance(stock_ids, StrictIterable) else stock_ids
		}

		if channels is not None:
			QueryMatchSchema.apply_schema(query, 'channels', channels)

		return query
