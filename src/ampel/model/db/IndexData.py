#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/IndexData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 21.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Optional, Dict
from ampel.model.BetterConfigDefaults import BetterConfigDefaults
from ampel.model.db.FieldData import FieldData

class IndexData(BaseModel):
	""" """
	Config = BetterConfigDefaults

	index: Optional[List[FieldData]]
	dbField: Optional[str]
	args: Dict = None

	# pylint: disable=arguments-differ,unused-argument
	def dict(self, **kwargs) -> Dict:
		if self.dbField:
			return {
				"index": [(self.dbField, 1)],
				"args": self.args
			} if self.args else {"index": [(self.dbField, 1)]}
		return super().dict(**kwargs)


	def get_id(self) -> str:
		""" 
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		# Shortcut
		if self.dbField:
			return FieldData(dbField=self.dbField).get_id()

		return "_".join(el.get_id() for el in self.index)
