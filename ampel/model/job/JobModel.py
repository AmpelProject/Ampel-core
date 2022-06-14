import re

from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from ampel.model.job.ExpressionParser import ExpressionParser
from typing import Optional, Any, Union, Literal

from pydantic import BaseModel, validator
from ampel.util.recursion import walk_and_process_dict


class TaskUnitModel(UnitModel):
    title: Optional[str]
    multiplier: int = 1


class TemplateUnitModel(BaseModel):
    title: Optional[str]
    template: str
    config: dict[str, Any]
    multiplier: int = 1


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
    def _resolve_parameters(cls, path, k, d, **kwargs):
        for k, v in d.items():
            if isinstance(v, str):
                chunks = []
                pos = 0
                for match in re.finditer(r"\{\{(.*)\}\}", v):
                    if match.span()[0] > pos:
                        chunks.append(v[pos : match.span()[0]])
                    chunks.append(
                        ExpressionParser.evaluate(
                            match.groups(1)[0].strip(), kwargs.get("parameters", {})
                        )
                    )
                    pos = match.span()[1]
                if pos < len(v):
                    chunks.append(v[pos : len(v)])
                d[k] = "".join(chunks)

    @validator("task", pre=True)
    def resolve_parameters(cls, v, values):
        parameters = {param.name: param.value for param in values.get("parameters", [])}
        walk_and_process_dict(
            v, callback=cls._resolve_parameters, parameters=parameters
        )
        return v
