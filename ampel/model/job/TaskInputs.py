#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/TaskInputs.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.job.InputParameter import InputParameter
from ampel.model.job.InputArtifact import InputArtifact

class TaskInputs(AmpelBaseModel):
    parameters: list[InputParameter] = []
    artifacts: list[InputArtifact] = []
