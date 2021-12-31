#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                ampel/model/builder/BuilderAliasModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.10.2019
# Last Modified Date:  11.10.2019
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Optional
from ampel.base.AmpelBaseModel import AmpelBaseModel

class BuilderAliasModel(AmpelBaseModel):

	t0: Optional[dict[str, Any]]
	t1: Optional[dict[str, Any]]
	t2: Optional[dict[str, Any]]
	t3: Optional[dict[str, Any]]
