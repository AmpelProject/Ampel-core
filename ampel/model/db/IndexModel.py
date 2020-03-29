#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/IndexModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 21.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Optional, Dict
from ampel.config.pydantic import BetterConfigDefaults
from ampel.model.db.FieldModel import FieldModel

class IndexModel(BaseModel):
	""" """
	Config = BetterConfigDefaults

	index: Optional[List[FieldModel]]
	db_field: Optional[str]
	args: Dict = None

	# pylint: disable=arguments-differ,unused-argument
	def dict(self, **kwargs) -> Dict:
		if self.db_field:
			return {
				"index": [(self.db_field, 1)],
				"args": self.args
			} if self.args else {"index": [(self.db_field, 1)]}
		return super().dict(**kwargs)


	def get_id(self) -> str:
		""" 
		Returns an indexId similar to what pymongo index_information outputs.
		Ex: [('tranId', 1), ('channel', 1)] -> tranId_1_channel_1
		"""
		# Shortcut
		if self.db_field:
			return FieldModel(db_field=self.db_field).get_id()

		return "_".join(el.get_id() for el in self.index)
