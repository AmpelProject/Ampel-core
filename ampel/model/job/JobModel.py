import re
from functools import partial
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union

from pydantic import BaseModel, validator

from ampel.model.ChannelModel import ChannelModel
from ampel.model.job.ExpressionParser import ExpressionParser
from ampel.model.UnitModel import UnitModel
from ampel.util.recursion import walk_and_process_dict


class OutputParameterSource(BaseModel):
    default: Optional[str]
    path: Path


class OutputParameter(BaseModel):
    name: str
    value_from: OutputParameterSource

    def value(self) -> Optional[str]:
        try:
            return self.value_from.path.read_text()
        except FileNotFoundError:
            return self.value_from.default


class TaskOutputs(BaseModel):
    parameters: list[OutputParameter] = []


class InputArtifactHttpSource(BaseModel):
    url: str


class InputArtifact(BaseModel):
    name: str
    path: Path
    http: InputArtifactHttpSource

    def value(self) -> Optional[str]:
        try:
            return self.path.read_text()
        except FileNotFoundError:
            return None


class InputParameter(BaseModel):
    name: str
    value: str


class TaskInputs(BaseModel):
    parameters: list[InputParameter] = []
    artifacts: list[InputArtifact] = []


class TaskUnitModel(UnitModel):
    title: str = ""
    multiplier: int = 1
    inputs: TaskInputs = TaskInputs()
    outputs: TaskOutputs = TaskOutputs()

    @validator("title", pre=True)
    def populate_title(cls, v, values):
        return v or values["unit"]


class TemplateUnitModel(BaseModel):
    title: str = ""
    template: str
    config: dict[str, Any]
    multiplier: int = 1
    inputs: TaskInputs = TaskInputs()
    outputs: TaskOutputs = TaskOutputs()

    @validator("title", pre=True)
    def populate_title(cls, v, values):
        return v or values["template"]


class MongoOpts(BaseModel):
    reset: bool = False
    prefix: str = "Ampel"


class Parameter(BaseModel):
    name: str
    value: str


class JobModel(BaseModel):
    name: str
    requirements: list[str] = []
    channel: list[dict[str, Any]] = []
    alias: dict[Literal["t0", "t1", "t2", "t3"], Any] = {}
    parameters: list[Parameter] = []
    mongo: MongoOpts = MongoOpts()
    task: list[Union[TemplateUnitModel, TaskUnitModel]]

    @validator("channel", each_item=True)
    def get_channel(cls, v):
        # work around incompatible AmpelBaseModel validation
        if not isinstance(v, dict):
            raise TypeError("channel must be a dict")
        if not "channel" in v:
            v["channel"] = v.pop("name")
        ChannelModel(**v)
        return v

    @classmethod
    def _transform_item(cls, v: str, transform: Callable[[str], str]) -> str:
        chunks = []
        pos = 0
        for match in re.finditer(r"\{\{(.*)\}\}", v):
            if match.span()[0] > pos:
                chunks.append(v[pos : match.span()[0]])
            chunks.append(transform(match.groups()[0].strip()))
            pos = match.span()[1]
        if pos < len(v):
            chunks.append(v[pos : len(v)])
        return "".join(chunks)

    @classmethod
    def _transform_expressions_callback(cls, path, k, d, **kwargs):
        for k, v in d.items():
            if isinstance(v, str):
                d[k] = cls._transform_item(v, kwargs["transform"])
            elif isinstance(v, list):
                d[k] = [
                    cls._transform_item(vv, kwargs["transform"])
                    if isinstance(vv, str)
                    else vv
                    for vv in v
                ]

    @classmethod
    def transform_expressions(
        cls, task_dict: dict, transformation: Callable[[str], str]
    ) -> dict:
        """
        Replace any expressions of the form {{ expr }} with the result of
        transformation(expr)
        """
        walk_and_process_dict(
            task_dict,
            callback=cls._transform_expressions_callback,
            transform=transformation,
        )
        return task_dict

    def resolve_expressions(self, target: dict, task: Union[TaskUnitModel,TemplateUnitModel]) -> dict:
        """
        Resolve any expressions of the form {{ expr }} found in string values of
        the target dict

        Supported expressions:

        - job parameters, e.g. {{ job.parameters.param }} resolves to the
          job-level parameter "param"
        - task outputs, e.g. {{ task.step-0.outputs.parameters.token }} resolves
          to the contents of the output file declared as "token" by the task
          named "step-0"
        """

        context = {
            "job": {
                "parameters": {param.name: param.value for param in self.parameters}
            },
            "task": {
                task.title: {
                    "outputs": {
                        "parameters": {
                            spec.name: spec
                            for spec in task.outputs.parameters
                        }
                    }
                }
                for task in self.task
            },
            "inputs": {
                "parameters": {param.name: param.value for param in task.inputs.parameters},
                "artifacts": {spec.name: spec for spec in task.inputs.artifacts},
            }
        }
        return self.transform_expressions(
            target,
            transformation=partial(ExpressionParser.evaluate, context=context),
        )
