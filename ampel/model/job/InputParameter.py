#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/InputParameter.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from ampel.base.AmpelBaseModel import AmpelBaseModel

class InputParameter(AmpelBaseModel):
    name: str
    value: str
