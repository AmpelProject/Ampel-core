#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBResultOrganizer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.02.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes

class DBResultOrganizer:
	"""
	"""

	@staticmethod
	def get_lightcurve_constituents(doc_list, tran_id=None, compound_id=None):
		"""
		Extract documents required to create an ampel.base.LightCurve object, i.e:
			-> Multiple photoPoints dicts
			-> A compound document
		"""
		compound_dict = None
		pps_dict = {}

		# Loop through query results
		for doc in doc_list:

			if tran_id is not None and doc['tranId'] != tran_id:
				continue

			# Pick up compound document 
			if doc["alDocType"] == AlDocTypes.COMPOUND:
				if compound_id is None or doc["_id"] == compound_id:
					compound_dict = doc

			# Pick up photopoint dictionaries
			if doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				pps_dict[doc['_id']] = doc

		return pps_dict, compound_dict



	@staticmethod
	def organize(doc_list, tran_id=None, photopoints=True, compounds=True, t2records=True, transient=True):
		"""
		"""

		if photopoints is False and compounds is False and t2records is False:
			raise ValueError("Nothing to extract")

		res = {}

		if photopoints is True:
			res['photopoints'] = []

		if compounds is True:
			res['compounds'] = []

		if t2records is True:
			res['t2records'] = []

		# Loop through query results
		for doc in doc_list:

			if tran_id is not None and doc['tranId'] != tran_id:
				continue

			# Pick up photopoint dicts
			if photopoints is True and doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				res['photopoints'].append(doc)

			# Pick up compound dicts 
			if compounds is True and doc["alDocType"] == AlDocTypes.COMPOUND:
				res['compounds'].append(doc)

			# Pick up t2 records
			if t2records is True and doc["alDocType"] == AlDocTypes.T2RECORD:
				res['t2records'].append(doc)

			# Pick up transient document
			if transient is True and doc["alDocType"] == AlDocTypes.TRANSIENT:
				res['transient'] = doc

		return res
