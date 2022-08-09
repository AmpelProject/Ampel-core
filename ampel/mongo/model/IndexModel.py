#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/db/IndexModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.10.2019
# Last Modified Date:  13.04.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Sequence
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.mongo.model.FieldModel import FieldModel

class IndexModel(AmpelBaseModel):

	index: Sequence[FieldModel]
	args: None | dict

	def get_id(self) -> str:
		"""
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		return "_".join(el.get_id() for el in self.index)
