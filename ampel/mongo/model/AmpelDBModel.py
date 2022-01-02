#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/db/AmpelDBModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.10.2019
# Last Modified Date:  08.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Sequence
from ampel.mongo.model.AmpelColModel import AmpelColModel
from ampel.mongo.model.MongoClientRoleModel import MongoClientRoleModel
from ampel.base.AmpelBaseModel import AmpelBaseModel

class AmpelDBModel(AmpelBaseModel):
	name: str
	collections: Sequence[AmpelColModel]
	role: MongoClientRoleModel
