#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t2/PhotoPointTimeSerie.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 19.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.pipeline.t0.AlertFlags import AlertFlags
from werkzeug.datastructures import ImmutableDict, ImmutableList


class PhotoPointTimeSerie:
	"""
		ALPHA CLASS. do not use
	"""
	
	@classmethod
	def set_mongo(cls, mongo_client):
		db = mongo_client["Ampel"]
		cls.col_pps = db["photopoints"]
		cls.col_tran = db["transients"]
		cls.col_t2 = db["t2"]


	def set_photo_points(self, photo_points):
		""" Setter method for the internal list of photo_points """
		self.pps = photo_points


	def set_compound(self, compound):
		""" Setter method for the internal (transient) compound structure """
		self.compound = compound


	def __init__(self, transient_id, compound=None, compound_id=None, photo_points=None):

		self.tran_id = transient_id
		self.param_id = param_id
		self.compound_id = compound_id

	
		t2doc = PhotoPointTimeSerie.col_t2.find(
			{	"t2Module": t2_module.value, 
				"paramId": paramId, 
				"compoundId": compoundId,
			}
		)
