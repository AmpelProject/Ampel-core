#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/db/FieldModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.10.2019
# Last Modified Date:  13.04.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelBaseModel import AmpelBaseModel

class FieldModel(AmpelBaseModel):

	field: str
	direction: int = 1

	def dict(self, **kwargs):
		return (self.field, self.direction)

	def get_id(self) -> str:
		"""
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		# Shortcut
		return f"{self.field}_{self.direction}"
