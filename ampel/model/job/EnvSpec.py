#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/EnvSpec.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  23.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelBaseModel import AmpelBaseModel

class EnvSpec(AmpelBaseModel):
    set: dict[str, str] = {}
    check: dict[str, int|str] = {}
