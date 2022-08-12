#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/InputArtifact.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from pathlib import Path
from pydantic import root_validator
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.job.InputArtifactHttpSource import InputArtifactHttpSource
from ampel.model.job.utils import transform_expressions

class InputArtifact(AmpelBaseModel):

    name: str
    path: Path
    http: InputArtifactHttpSource

    def value(self) -> None | str:
        try:
            return self.path.read_text()
        except FileNotFoundError:
            return None

    @staticmethod
    def _check_expression(expression: str):
        """For compatibility with Argo, forbid use of {{ item }} in artifact specs"""
        if expression == "item":
            raise ValueError("artifact fields cannot reference {{ item }}")
        return expression

    @root_validator
    def check_for_item(cls, values):
        transform_expressions(values, cls._check_expression)
        return values
