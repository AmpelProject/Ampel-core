#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/db/MongoClientRoleModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 13.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.model.StrictModel import StrictModel

class MongoClientRoleModel(StrictModel):
	r: str
	w: str
