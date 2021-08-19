#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/t2.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.02.2018
# Last Modified Date: 27.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.binary import Binary
from typing import Union, Dict, Any, Optional, List
from ampel.types import StockId, ChannelId, StrictIterable, strict_iterable
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.util.collections import check_seq_inner_type
from ampel.mongo.utils import maybe_match_array
from ampel.mongo.query.general import build_general_query


def build_stateless_query(
	stock: Optional[Union[StockId, StrictIterable[StockId]]] = None,
	channel: Optional[Union[ChannelId, Dict, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]]] = None,
	t2_subsel: Union[int, str, StrictIterable[Union[int, str]]] = None
) -> Dict[str, Any]:
	"""
	| Builds a pymongo query dict aiming at loading transient t2 or compounds docs \
	| Stateless query: all avail compounds and t2docs (although possibly \
	constrained by parameter t2_subsel) are targeted.

	:param stock: (query can be performed on multiple ids at once)

	:param channel: see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` for details. \
	None (no criterium) means all channels are considered.

	:param t2_subsel: optional sub-selection of t2 records based on t2 class names. \
	-> only t2 records matching with the provided t2 class names will be returned. \
	If None (or empty iterable): all t2 docs associated with the matched transients will be targeted. \
	"""

	query = build_general_query(stock=stock, channel=channel)

	if t2_subsel:
		query['unit'] = t2_subsel if isinstance(t2_subsel, (str,int)) else maybe_match_array(t2_subsel)

	return query


def build_statebound_t1_query(
	states: Union[str, bytes, Binary, StrictIterable[Union[str, bytes, Binary]]],
) -> Dict[str, Any]:
	return {'_id': get_compound_match(states)}


def build_statebound_t2_query(
	stock: Optional[Union[StockId, StrictIterable[StockId]]],
	states: Union[str, bytes, Binary, StrictIterable[Union[str, bytes, Binary]]],
	channel: Optional[Union[ChannelId, Dict, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]]] = None,
	t2_subsel: Union[int, str, StrictIterable[Union[int, str]]] = None
) -> Dict[str, Any]:
	"""
	See :func:`build_stateless_query <build_stateless_query>` docstring
	"""

	query = build_general_query(stock=stock, channel=channel)
	query['link'] = get_compound_match(states)

	if t2_subsel:
		query['unit'] = t2_subsel if isinstance(t2_subsel, (str,int)) else maybe_match_array(t2_subsel)

	return query

def _to_binary(st: Union[str, bytes, Binary]) -> Binary:
	if isinstance(st, str):
		if len(st) == 32:
			return Binary(bytes.fromhex(st), 0)
		else:
			raise ValueError("Provided state strings must have 32 characters")
	elif isinstance(st, bytes):
		if len(st) == 16:
			return Binary(st, 0)
		else:
			raise ValueError("Provided state bytes must have a length of 16")
	else:
		if st.subtype == 0:
			return st
		else:
			raise ValueError("Bson Binary states must have subtype 0")


def get_compound_match(
	states: Union[str, bytes, Binary, StrictIterable[Union[str, bytes, Binary]]],
) -> Union[Binary, Dict[str, List[Binary]]]:
	"""
	:raises ValueError: if provided states parameter is invalid
	"""

	# Single state was provided
	if isinstance(states, (str, bytes, Binary)):
		return _to_binary(states)

	# Multiple states were provided
	elif isinstance(states, strict_iterable):

		return {
			'$in': [_to_binary(st) for st in states]
		}

	else:
		raise ValueError(
			f"Type of provided state ({type(states)}) must be "
			f"bytes, str, bson.Binary or sequences of these"
		)
