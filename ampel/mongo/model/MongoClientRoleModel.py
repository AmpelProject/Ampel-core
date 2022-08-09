#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/db/MongoClientRoleModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.10.2019
# Last Modified Date:  13.04.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelBaseModel import AmpelBaseModel

class MongoClientRoleModel(AmpelBaseModel):
	r: str
	w: str
