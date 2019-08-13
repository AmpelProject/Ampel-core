#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.02.2019
# Last Modified Date: 21.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, sys

class DBUtils:

	@staticmethod
	def b2_hash(val):
		"""
		No collision occured applying blake2 using 7bytes digests on the word list
		https://github.com/dwyl/english-words/blob/master/words.txt
		containing 466544 english words

		:param val: str
		:returns: 7bytes int (using 8 bytes yields a mongodb OverflowError)
		"""
		return int.from_bytes(
			# don't undestand why pylint complains about  digest_size
			# pylint: disable=unexpected-keyword-arg
			hashlib.blake2b(
				bytes(val, "utf-8"), 
				digest_size=7 # using 8 bytes yields a mongodb OverflowError
			).digest(), 
			byteorder=sys.byteorder
		)
