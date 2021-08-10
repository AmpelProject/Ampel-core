#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/db/IndexModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 13.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional, Dict
from ampel.model.StrictModel import StrictModel
from ampel.model.db.FieldModel import FieldModel

class IndexModel(StrictModel):

	index: List[FieldModel]
	args: Optional[Dict]

	def get_id(self) -> str:
		"""
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		return "_".join(el.get_id() for el in self.index)
