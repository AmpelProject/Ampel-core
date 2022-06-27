import re
from functools import partial
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union

from pydantic import BaseModel, validator, root_validator

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


class ExpandWithItems(BaseModel):
    items: list

    def __iter__(self):
        return iter(self.items)


class BaseSequence(BaseModel):
    start: int = 0
    format: str = "%i"


class SequenceWithEnd(BaseSequence):
    end: int

    def items(self):
        yield from range(self.start, self.end)


class SequenceWithCount(BaseSequence):
    count: int

    def items(self):
        yield from range(self.start, self.start+self.count)


class ExpandWithSequence(BaseModel):
    sequence: Union[SequenceWithCount, SequenceWithEnd]

    def items(self):
        for i in self.sequence.items():
            yield self.sequence.format % i
    
    def __iter__(self):
        return self.items()


ExpandWith = Union[None, ExpandWithItems, ExpandWithSequence]


def _parse_multiplier(values: dict[str,Any]) -> dict:
    if not isinstance(multiplier := values.pop("multiplier", 1), int):
        raise TypeError("multiplier must be an int")
    if multiplier > 1:
        assert "expand_with" not in values, "multiplier and expand_with may not be used together"
        values |= {"expand_with": {"sequence": {"count": multiplier}}}
    return values


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

class TemplateUnitModel(BaseModel):
    title: str = ""
    template: str
    config: dict[str, Any]
    inputs: TaskInputs = TaskInputs()
    outputs: TaskOutputs = TaskOutputs()
    expand_with: ExpandWith = None

    @validator("title", pre=True)
    def populate_title(cls, v, values):
        return v or values["template"]

    @root_validator(pre=True)
    def parse_multiplier(cls, values):
        return _parse_multiplier(values)

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

    def resolve_expressions(
        self,
        target: dict,
        task: Union[TaskUnitModel, TemplateUnitModel],
        item: Union[None, str, dict, list] = None,
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
        return self.transform_expressions(
            target,
            transformation=partial(ExpressionParser.evaluate, context=context),
        )
