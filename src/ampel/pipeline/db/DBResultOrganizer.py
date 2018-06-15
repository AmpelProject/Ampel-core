#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBResultOrganizer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.02.2018
# Last Modified Date: 29.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
import bson

class DBResultOrganizer:
	"""
	"""

	@staticmethod
	def organize(
		doc_list, tran_id=None, photopoints=True, upperlimits=True, 
		compounds=True, t2records=True, transient=True
	):
		"""
		Build the following dict structure using the documents avail in doc_list:
		{
			"photopoints": [list of dicts], 
			"upperlimits": [list of dicts], 
			"compounds": [list of dicts], 
			"t2records": [list of dicts], 
			"transient": dict, 
		}

		If you don't provide 'tran_id', then 'doc_list' must only contain 
		docs related to one transient.
		"""

		if photopoints is False and compounds is False and t2records is False:
			raise ValueError("Nothing to extract")

		res = {}

		res['photopoints'] = [] if photopoints is True else None
		res['upperlimits'] = [] if upperlimits is True else None
		res['compounds'] = [] if compounds is True else None
		res['t2records'] = [] if t2records is True else None

		if transient is False:
			res['transient'] = None

		# Loop through query results
		for doc in doc_list:

			if tran_id is not None and doc['tranId'] != tran_id:
				continue

			# Pick up photo point dicts
			if photopoints is True and type(doc["_id"]) is bson.int64.Int64 and doc["_id"] > 0:
				res['photopoints'].append(doc)
				continue

			# Pick up upper limit dicts
			if upperlimits is True and type(doc["_id"]) is bson.int64.Int64 and doc["_id"] < 0:
				res['upperlimits'].append(doc)
				continue

			# Pick up compound dicts 
			if compounds is True and "alDocType" in doc and doc["alDocType"] == AlDocTypes.COMPOUND:
				res['compounds'].append(doc)
				continue

			# Pick up t2 records
			if t2records is True and "alDocType" in doc and doc["alDocType"] == AlDocTypes.T2RECORD:
				res['t2records'].append(doc)
				continue

			# Pick up transient document
			if transient is True and "alDocType" in doc and doc["alDocType"] == AlDocTypes.TRANSIENT:
				if 'transient' in res:
					raise ValueError(
						"doc_list contains docs related to multiple transients "+
						"but parameter tran_id was not set"
					)
				res['transient'] = doc
				continue

		return res
