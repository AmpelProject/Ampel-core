import re

from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from ampel.model.job.ExpressionParser import ExpressionParser
from typing import Optional, Any, Union, Literal

from pydantic import BaseModel, validator
from ampel.util.recursion import walk_and_process_dict


class OutputParameterSource(BaseModel):
    default: Optional[str]
    path: str


class OutputParameter(BaseModel):
    name: str
    value_from: OutputParameterSource


class TaskOutputs(BaseModel):
    parameters: list[OutputParameter] = []


class TaskUnitModel(UnitModel):
    title: str = ""
    multiplier: int = 1
    outputs: TaskOutputs = TaskOutputs()

    @validator("title", pre=True)
    def populate_title(cls, v, values):
        return v or values["unit"]


class TemplateUnitModel(BaseModel):
    title: str = ""
    template: str
    config: dict[str, Any]
    multiplier: int = 1
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

    def _resolve_expressions_callback(self, path, k, d, **kwargs):
        for k, v in d.items():
            if isinstance(v, str):
                chunks = []
                pos = 0
                for match in re.finditer(r"\{\{(.*)\}\}", v):
                    if match.span()[0] > pos:
                        chunks.append(v[pos : match.span()[0]])
                    chunks.append(
                        ExpressionParser.evaluate(
                            match.groups(1)[0].strip(), kwargs.get("context", {})
                        )
                    )
                    pos = match.span()[1]
                if pos < len(v):
                    chunks.append(v[pos : len(v)])
                d[k] = "".join(chunks)

    def resolve_expressions(self, task_dict: dict) -> dict:
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
        def _read_output(spec: OutputParameterSource) -> Optional[str]:
            try:
                with open(spec.path) as f:
                    return f.read()
            except FileNotFoundError:
                return spec.default

        context = {
            "job": {
                "parameters": {param.name: param.value for param in self.parameters}
            },
            "task": {
                task.title: {
                    "outputs": {
                        "parameters": {
                            spec.name: value
                            for spec in task.outputs.parameters
                            if (value := _read_output(spec.value_from)) is not None
                        }
                    }
                }
                for task in self.task
            },
        }
        walk_and_process_dict(
            task_dict, callback=self._resolve_expressions_callback, context=context
        )
        return task_dict
