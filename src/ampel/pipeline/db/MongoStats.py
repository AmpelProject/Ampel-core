#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/MongoStats.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 23.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>



class MongoStats:
	"""
	"""


	@staticmethod
	def col_stats(col, use_dict=None):

		colstats = col.database.command(
			"collstats", col.name
		)

		if use_dict == None:
			use_dict = {}

		for key in ['count', 'size', 'storageSize', 'totalIndexSize']:
			use_dict[key] = colstats[key]

		return use_dict
