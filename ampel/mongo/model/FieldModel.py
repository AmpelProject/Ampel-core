#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/db/FieldModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.10.2019
# Last Modified Date:  13.04.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal, TypeAlias

from pydantic import model_serializer

from ampel.base.AmpelBaseModel import AmpelBaseModel

IndexType: TypeAlias = Literal[1,-1,'2d','2dsphere','hashed','text']

class FieldModel(AmpelBaseModel):

	field: str
	direction: IndexType = 1

	@model_serializer
	def to_tuple(self) -> tuple[str, IndexType]:
		return (self.field, self.direction)

	def get_id(self) -> str:
		"""
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		# Shortcut
		return f"{self.field}_{self.direction}"
