#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/MongoStats.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 13.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce
from ampel.flags.AlDocTypes import AlDocTypes

class MongoStats:
	"""
	"""

	# mongod serverStatus key values
	db_metrics = {
		"mem.resident": "memRes",
		"metrics.document.deleted": "docDel",
		"metrics.document.inserted": "docIns",
		"metrics.document.returned": "docRet",
		"metrics.document.updated": "docUpd"
	}


	@classmethod
	def get_server_stats(cls, db, ret_dict=None, suffix=""):
		"""
		"""
		if ret_dict == None:
			ret_dict = {}

		server_status = db.command("serverStatus")
		for k, v in cls.db_metrics.items():
			ret_dict[suffix + v] = reduce(dict.get, k.split("."), server_status)

		return ret_dict


	@staticmethod
	def get_col_stats(col, ret_dict=None, suffix=""):
		"""
		"""
		colstats = col.database.command(
			"collstats", col.name
		)

		if ret_dict == None:
			ret_dict = {}

		for key in ('count', 'size', 'storageSize', 'totalIndexSize'):
			ret_dict[suffix + key] = colstats[key]

		return ret_dict


	@staticmethod
	def get_tran_count(col, channel_name=None):
		"""
		get number of unique transient in collection

		channel_name:
			-> if None: get total number of unique transient in collection
			-> if spectified: get total number of unique transient for the specified channel in collection
		"""

		if channel_name is None:

			return col.find(
				{
					'alDocType': AlDocTypes.TRANSIENT
				}
			).count()

		else:

			return col.find(
				{
					'alDocType': AlDocTypes.TRANSIENT, 
					'channels': channel_name
				}
			).count()
