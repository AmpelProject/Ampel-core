#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/MongoClientRoleData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.model.BetterConfigDefaults import BetterConfigDefaults

class MongoClientRoleData(BaseModel):
	""" """
	Config = BetterConfigDefaults

	r: str
	w: str
