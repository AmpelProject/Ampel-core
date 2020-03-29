#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/FieldModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.config.pydantic import BetterConfigDefaults

class FieldModel(BaseModel):
	""" """
	Config = BetterConfigDefaults

	db_field: str
	direction: int = 1

	# pylint: disable=arguments-differ,unused-argument
	def dict(self, **kwargs):
		return (self.db_field, self.direction)

	def get_id(self) -> str:
		""" 
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		# Shortcut
		return f"{self.db_field}_{self.direction}"
