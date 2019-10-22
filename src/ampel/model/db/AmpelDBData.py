#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/AmpelDBData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Sequence
from ampel.model.db.AmpelColData import AmpelColData
from ampel.model.db.MongoClientRoleData import MongoClientRoleData
from ampel.model.BetterConfigDefaults import BetterConfigDefaults

class AmpelDBData(BaseModel):
	""" """
	Config = BetterConfigDefaults

	name: str
	collections: Sequence[AmpelColData]
	role: MongoClientRoleData
