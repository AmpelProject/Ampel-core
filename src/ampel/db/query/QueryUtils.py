#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/query/QueryUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.10.2018
# Last Modified Date: 31.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.common.AmpelUtils import AmpelUtils


class QueryUtils:


	@staticmethod
	def match_array(arg):
		"""
		match_array(['ab']) -> returns 'ab'
		match_array({'ab'}) -> returns 'ab'
		match_array(['a', 'b']) -> returns {$in: ['a', 'b']}
		match_array({'a', 'b'}) -> returns {$in: ['a', 'b']}
		"""

		if not AmpelUtils.is_sequence(arg):
			raise ValueError("Provided argument is not sequence (%s)" % type(arg))

		if len(arg) == 1:
			return next(iter(arg))

		if type(arg) is set:
			return {'$in' : list(arg)}

		return {'$in' : arg}
