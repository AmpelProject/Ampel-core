#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/general.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.12.2019
# Last Modified Date: 17.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.int64 import Int64
from typing import Dict, Optional, Union, Any, Literal
from ampel.types import Tag, ChannelId, StockId, StrictIterable
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.mongo.utils import maybe_match_array
from ampel.mongo.schema import apply_schema, apply_excl_schema

type_stock_id = (int, Int64, bytes, str)


def build_general_query(
	stock: Optional[Union[StockId, StrictIterable[StockId]]] = None,
	channel: Optional[Union[ChannelId, Dict, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]]] = None,
	tag: Optional[Dict[Literal['with', 'without'], Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]]] = None
) -> Dict[str, Any]:
	"""
	Builds a query usable with the ampel "stock", "t0" (with channel=None), "t1" and "t2" collections
	:param stock: matching multiple ids with a single query is possible
	:param channel: None (no criterium) means all channel are considered.
	:param tag: tags to be (or not to be) matched by query
	:returns: query dict with matching criteria
	:raises ValueError: apply_schema can raise ValueError in case the provided dict schema structure is unsupported
	"""

	query = {}

	if stock:
		query['stock'] = stock if isinstance(stock, type_stock_id) \
			else maybe_match_array(stock) # type: ignore[arg-type]

	if channel:
		apply_schema(query, 'channel', channel)

	if tag:

		if 'with' in tag:
			apply_schema(query, 'tag', tag['with'])

		# Order matters, parse_dict(...) must be called *after* parse_excl_dict(...)
		if 'without' in tag:
			apply_excl_schema(query, 'tag', tag['without'])

	return query
