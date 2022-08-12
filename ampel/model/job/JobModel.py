#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/JobModel.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from functools import partial
from typing import Any, Literal
from pydantic import validator

from ampel.model.ChannelModel import ChannelModel
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.job.ExpressionParser import ExpressionParser
from ampel.model.job.MongoOpts import MongoOpts
from ampel.model.job.TemplateUnitModel import TemplateUnitModel
from ampel.model.job.TaskUnitModel import TaskUnitModel
from ampel.model.job.EnvSpec import EnvSpec
from ampel.model.job.Parameter import Parameter
from ampel.model.job.utils import transform_expressions


class JobModel(AmpelBaseModel):

    name: None | str
    requirements: list[str] = []
    env: dict[str, EnvSpec] = {}
    channel: list[dict[str, Any]] = []
    alias: dict[Literal["t0", "t1", "t2", "t3"], dict[str, Any]] = {}
    parameters: list[Parameter] = []
    mongo: MongoOpts = MongoOpts()
    task: list[TemplateUnitModel | TaskUnitModel]


    @validator("channel", each_item=True)
    def get_channel(cls, v):
        # work around incompatible AmpelBaseModel validation
        if not isinstance(v, dict):
            raise TypeError("channel must be a dict")
        if "channel" not in v:
            v["channel"] = v.pop("name")
        ChannelModel(**v)
        return v


    def resolve_expressions(
        self,
        target: dict,
        task: TaskUnitModel | TemplateUnitModel,
        item: None | str | dict | list = None
    ) -> dict:
        """
        Resolve any expressions of the form {{ expr }} found in string values of
        the target dict

        Supported expressions:

        - job parameters, e.g. {{ job.parameters.param }} resolves to the
          job-level parameter "param"
        - task outputs, e.g. {{ task.step-0.outputs.parameters.token }} resolves
          to the contents of the output file declared as "token" by the task
          named "step-0"
        - iteration items, e.g. {{ item }} for a string-valued item or {{
          item.name }} for dict-valued
        """

        context = {
            "job": {
                "parameters": {param.name: param.value for param in self.parameters}
            },
            "task": {
                task.title: {
                    "outputs": {
                        "parameters": {
                            spec.name: spec.value_from.path
                            for spec in task.outputs.parameters
                        }
                    }
                }
                for task in self.task
            },
            "inputs": {
                "parameters": {
                    param.name: param.value for param in task.inputs.parameters
                },
                "artifacts": {spec.name: spec.path for spec in task.inputs.artifacts},
            },
            "item": item,
        }

        return transform_expressions(
            target,
            transformation=partial(ExpressionParser.evaluate, context=context),
        )
