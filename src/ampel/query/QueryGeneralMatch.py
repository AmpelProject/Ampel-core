#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/query/QueryGeneralMatch.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.12.2019
# Last Modified Date: 11.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Union, Any, Iterable

from ampel.typing import StrictIterable
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.query.QueryUtils import QueryUtils
from ampel.query.QueryMatchSchema import QueryMatchSchema


class QueryGeneralMatch:
	"""
	Builds a query usable with the ampel "stock", "t0" (with channels=None), 
	"t1" and "t2" collections
	"""

	@classmethod
	def build(cls,
		stock_ids: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
		channels: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		with_tags: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
		without_tags: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None,
	) -> Dict[str, Any]:
		"""
		:param stock_ids: matching multiple ids with a single query is possible

		:param channels: \
			a single channel or a dict schema \
			(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` for details). \
			None (no criterium) means all channels are considered. 

		:param with_tags: \
			"tags" to be matched by query \
			(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` syntax details). \

		:param without_tags: \
			"tags" not to be matched by query

		:returns: query dict with matching criteria
		"""

		query = {}


		if stock_ids:
			query['stockId'] = QueryUtils.match_array(stock_ids) \
				if isinstance(stock_ids, StrictIterable) else stock_ids

		if channels:
			QueryMatchSchema.apply_schema(
				query, 'channels', channels
			)

		if with_tags:
			QueryMatchSchema.apply_schema(
				query, 'alTags', with_tags
			)

		# Order matters, parse_dict(...) must be called *after* parse_excl_dict(...)
		if without_tags is not None:
			QueryMatchSchema.apply_excl_schema(
				query, 'alTags', without_tags
			)

		return query
