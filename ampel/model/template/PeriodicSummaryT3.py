#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/PeriodicSummaryT3.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.08.2020
# Last Modified Date: 10.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from logging import Logger
from typing import Any, Dict, Literal, Optional, Sequence, Union, List

from ampel.abstract.AbsProcessTemplate import AbsProcessTemplate
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.StrictModel import StrictModel
from ampel.model.t3.LoaderDirective import LoaderDirective
from ampel.model.t3.T2FilterModel import T2FilterModel
from ampel.model.UnitModel import UnitModel
from ampel.type import ChannelId, Tag


class FilterModel(StrictModel):
    t2: Union[T2FilterModel, AllOf[T2FilterModel], AnyOf[T2FilterModel]]

UnitModelOrString = Union[UnitModel, str]
UnitModelSequence = Union[UnitModelOrString, Sequence[UnitModelOrString]]

class PeriodicSummaryT3(AbsProcessTemplate):
    """
    A T3 process that selects stocks modified since its last invocation, and
    supplies them, unfiltered, to a sequence of AbsT3Units.
    """

    name: str
    tier: Literal[3] = 3
    active: bool = True
    schedule: Union[str, Sequence[str]]
    channel: Union[
        None, ChannelId, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]
    ] = None
    distrib: Optional[str]
    source: Optional[str]
    tag: Optional[
        Dict[
            Literal["with", "without"],
            Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]],
        ]
    ] = None
    load: Optional[Sequence[Union[str,LoaderDirective]]] = None
    filter: Optional[FilterModel] = None
    complement: Optional[UnitModelSequence] = None
    run: UnitModelSequence

    def get_process(self, logger: Logger) -> Dict[str, Any]:
        directive: Dict[str, Any] = {
            "context": [
                {"unit": "T3AddAlertsNumber"},
            ],
            "select": {
                "unit": "T3StockSelector",
                "config": {
                    "modified": {
                        "after": {
                            "match_type": "time_last_run",
                            "process_name": self.name,
                        },
                        "before": {"match_type": "time_delta"},
                    },
                    "channel": self.channel,
                    "tag": self.tag,
                },
            },
            # FIXME: use T3LatestStateLoader here by default
            "load": {"unit": "T3SimpleDataLoader"},
            "run": {
                "unit": "T3UnitRunner",
                "config": {
                    "directives": [{"execute": self.get_units(self.run)}]
                },
            },
        }

        # Restrict stock selection according to T2 values
        if self.filter:
            directive["select"]["unit"] = "T3FilteringStockSelector"
            directive["select"]["config"]["t2_filter"] = self.filter.t2.dict()

        # Restrict document types to load
        if self.load is not None:
            directive["load"]["config"] = {"directives": self.load}

        if self.complement is not None:
            directive["complement"] = self.get_units(self.complement)

        ret: Dict[str, Any] = {
            "tier": self.tier,
            "schedule": self.schedule,
            "active": self.active,
            "distrib": self.distrib,
            "source": self.source,
            "channel": self.get_channel_tag(),
            "name": self.name,
            "processor": {
                "unit": "T3Processor",
                "config": {"process_name": self.name, "directives": [directive]},
            },
        }

        return ret

    def get_units(self, units: UnitModelSequence) -> List[Dict[str,Any]]:
        if isinstance(units, str):
            return [UnitModel(unit=units).dict()]
        elif isinstance(units, UnitModel):
            return [units.dict()]
        else:
            return [self.get_units(u)[0] for u in units]

    def get_schedule(self) -> Sequence[str]:
        if isinstance(self.schedule, str):
            return [self.schedule]
        else:
            return self.schedule

    def get_channel_tag(self) -> Union[None, str, int]:
        """
        Get channel if single channel, otherwise None
        """
        if isinstance(self.channel, str) or isinstance(self.channel, int):
            return self.channel
        else:
            return None
