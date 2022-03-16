from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from typing import Optional, Any, Union, Literal

from pydantic import BaseModel, validator


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


class JobModel(BaseModel):
    name: str
    requirements: list[str] = []
    channel: list[dict[str, Any]] = []
    alias: dict[Literal["t0", "t1", "t2", "t3"], Any] = {}
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
