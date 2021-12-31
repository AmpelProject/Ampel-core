#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/db/ShortIndexModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.04.2020
# Last Modified Date: 16.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Any
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.mongo.model.FieldModel import FieldModel


class ShortIndexModel(AmpelBaseModel):

	field: str
	args: Optional[dict] = None

	def dict(self, **kwargs) -> dict[str, Any]:
		if self.args:
			return {
				"index": [(self.field, 1)],
				"args": self.args
			}
		return {"index": [(self.field, 1)]}


	def get_id(self) -> str:
		"""
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		return FieldModel(field=self.field).get_id()
