#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/OutputParameterSource.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from pathlib import Path
from ampel.base.AmpelBaseModel import AmpelBaseModel

class OutputParameterSource(AmpelBaseModel):
    default: None | str
    path: Path
