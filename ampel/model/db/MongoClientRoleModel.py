#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/MongoClientRoleModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.config.pydantic import BetterConfigDefaults

class MongoClientRoleModel(BaseModel):
	""" """
	Config = BetterConfigDefaults

	r: str
	w: str
