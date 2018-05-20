#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBResultOrganizer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.02.2018
# Last Modified Date: 20.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes

class DBResultOrganizer:
	"""
	"""

	@staticmethod
	def get_lightcurve_constituents(doc_list, tran_id=None, compound_id=None):
		"""
		Extract documents required to create an ampel.base.LightCurve object, i.e:
			-> Photo point list
			-> Upper limit list
			-> A compound document

		doc_list can be a list of dicts or a pymongo cursor.
		If doc_list contains documents from multiple transients, 
		please provide the target transient id by setting the parameter "tran_id".
		Also, if doc_list contains multiple 'states' for the provided transient,
		i.e if multiple compound documents are present in doc_list for the target
		transient, please provide the target compound_id.
		"""
		compound_dict = None
		pps_list = []
		uls_list = []

		# Loop through query results (or cursor)
		for doc in doc_list:

			if tran_id is not None and doc['tranId'] != tran_id:
				continue

			# Pick up compound document 
			if doc["alDocType"] == AlDocTypes.COMPOUND:
				if compound_id is None or doc["_id"] == compound_id:
					compound_dict = doc
				continue

			# Pick up photopoint dict
			if doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				pps_list.append(doc)
				continue

			# Pick up upper limit dict
			if doc["alDocType"] == AlDocTypes.UPPERLIMIT:
				uls_list.append(doc)

		return pps_list, uls_list, compound_dict



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

		if photopoints is True:
			res['photopoints'] = []

		if upperlimits is True:
			res['upperlimits'] = []

		if compounds is True:
			res['compounds'] = []

		if t2records is True:
			res['t2records'] = []

		# Loop through query results
		for doc in doc_list:

			if tran_id is not None and doc['tranId'] != tran_id:
				continue

			# Pick up photo point dicts
			if photopoints is True and doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				res['photopoints'].append(doc)
				continue

			# Pick up upper limit dicts
			if upperlimits is True and doc["alDocType"] == AlDocTypes.UPPERLIMIT:
				res['upperlimits'].append(doc)
				continue

			# Pick up compound dicts 
			if compounds is True and doc["alDocType"] == AlDocTypes.COMPOUND:
				res['compounds'].append(doc)
				continue

			# Pick up t2 records
			if t2records is True and doc["alDocType"] == AlDocTypes.T2RECORD:
				res['t2records'].append(doc)
				continue

			# Pick up transient document
			if transient is True and doc["alDocType"] == AlDocTypes.TRANSIENT:
				if 'transient' in res:
					raise ValueError(
						"doc_list contains docs related to multiple transients "+
						"but parameter tran_id was not set"
					)
				res['transient'] = doc
				continue

		return res
