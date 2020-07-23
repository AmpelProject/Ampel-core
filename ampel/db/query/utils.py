#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/query/utils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.10.2018
# Last Modified Date: 21.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.type import strict_iterable, StrictIterable


def add_or(query: Dict[str, Any], arg: Dict[str, Any]) -> None:

	if '$or' in query:
		if '$and' in query:
			if not isinstance(query['$and'], list):
				raise ValueError(f"Illegal $and value in query: {query}")
			query['$and'].append(arg)
		else:
			query['$and'] = [query.pop('$or'), arg]

	else:
		query['$or'] = arg


def match_array(arg: StrictIterable[Any]):
	"""
	match_array(['ab']) -> returns 'ab'
	match_array({'ab'}) -> returns 'ab'
	match_array(['a', 'b']) -> returns {$in: ['a', 'b']}
	match_array({'a', 'b'}) -> returns {$in: ['a', 'b']}
	"""

	if not isinstance(arg, strict_iterable):
		raise ValueError(
			f"Provided argument is not sequence ({type(arg)})"
		)

	if len(arg) == 1:
		return next(iter(arg))

	if isinstance(arg, list):
		return {'$in': arg}

	# Otherwise cast to list
	return {'$in': list(arg)}
