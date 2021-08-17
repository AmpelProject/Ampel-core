#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/db/AmpelDBModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 08.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence
from ampel.mongo.model.AmpelColModel import AmpelColModel
from ampel.mongo.model.MongoClientRoleModel import MongoClientRoleModel
from ampel.model.StrictModel import StrictModel

class AmpelDBModel(StrictModel):
	name: str
	collections: Sequence[AmpelColModel]
	role: MongoClientRoleModel
