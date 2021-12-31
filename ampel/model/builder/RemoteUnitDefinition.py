#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/builder/RemoteUnitDefinition.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                06.11.2019
# Last Modified Date:  15.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import List
from ampel.base.AmpelBaseModel import AmpelBaseModel

class RemoteUnitDefinition(AmpelBaseModel):

	class_name: str
	base: list['str']
