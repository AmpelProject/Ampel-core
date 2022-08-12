#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/TaskUnitModel.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from pydantic import validator, root_validator
from ampel.model.job.TaskInputs import TaskInputs
from ampel.model.job.TaskOutputs import TaskOutputs
from ampel.model.job.utils import ExpandWith, _parse_multiplier
from ampel.model.UnitModel import UnitModel


class TaskUnitModel(UnitModel):

    title: str = ""
    inputs: TaskInputs = TaskInputs()
    outputs: TaskOutputs = TaskOutputs()
    expand_with: ExpandWith = None

    @validator("title", pre=True)
    def populate_title(cls, v, values):
        return v or values["unit"]

    @root_validator(pre=True)
    def parse_multiplier(cls, values):
        return _parse_multiplier(values)
