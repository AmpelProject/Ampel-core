#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBIndexCreator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.05.2018
# Last Modified Date: 21.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.flags.AlDocTypes import AlDocTypes
import pymongo

class DBIndexCreator:
	""" 
	"""

	@staticmethod
	def create_tran_db_indexes(col):
	    col.create_index(
	        [
	            ("tranId", pymongo.ASCENDING), 
	            ("alDocType", pymongo.ASCENDING), 
	            ("channels", pymongo.ASCENDING)
	        ]
	    )

	@staticmethod
	def test_create_tran_db_indexes(col):

			# Create tranId index for all ampel doc types but TRANSIENT
			col.create_index(
				[("tranId", 1)],
				**{ 
					"partialFilterExpression": {
						"alDocType": {
							"$gt": 1
						}
					}
				}
			)

			# Create alDocType index
			col.create_index(
				[("alDocType", 1)],
			)

			# Create channels index
			#col.create_index(
			#	[("channels", 1)],
			#)

			# Create compound tranId index for TRANSIENT doctype
			# this enbales covered autocomplete queries at T0 level
			col.create_index(
				[
					("tranId", 1), 
					("alDocType", 1), 
					("channels", 1)
				],
				**{ 
					"partialFilterExpression": {
						"alDocType": AlDocTypes.TRANSIENT
					}
				}
			)

			# Create sparse runstate index
			col.create_index(
				[
					("runState", 1)
				],
				**{ 
					"partialFilterExpression": {
						"alDocType": AlDocTypes.T2RECORD
					}
				}
			)
