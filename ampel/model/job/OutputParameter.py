#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/OutputParameter.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.job.OutputParameterSource import OutputParameterSource

class OutputParameter(AmpelBaseModel):

    name: str
    value_from: OutputParameterSource

    def value(self) -> None | str:
        try:
            return self.value_from.path.read_text()
        except FileNotFoundError:
            return self.value_from.default
