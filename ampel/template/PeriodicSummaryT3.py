#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/template/PeriodicSummaryT3.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.08.2020
# Last Modified Date: 14.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Literal, Union
from collections.abc import Sequence

from ampel.types import OneOrMany, ChannelId, Tag
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.t3.LoaderDirective import LoaderDirective
from ampel.model.t3.T2FilterModel import T2FilterModel
from ampel.model.UnitModel import UnitModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsProcessTemplate import AbsProcessTemplate


class FilterModel(AmpelBaseModel):
    #: Filter based on T2 results
    t2: T2FilterModel | AllOf[T2FilterModel] | AnyOf[T2FilterModel]


class PeriodicSummaryT3(AbsProcessTemplate):
    """
    A T3 process that selects stocks updated since its last invocation, and
    supplies them, to a sequence of AbsT3ReviewUnits.
    """

    #: Process name.
    name: str

    tier: Literal[3] = 3

    active: bool = True

    #: one or more `schedule <https://schedule.readthedocs.io/en/stable/>`_
    #: expressions, e.g: ``every().day.at("15:00")`` or ``every(42).minutes``
    #:
    #: .. note:: all times are are expressed in UTC
    schedule: OneOrMany[str]

    #: Channel selection.
    channel: None | ChannelId | AllOf[ChannelId] | AnyOf[ChannelId] | OneOf[ChannelId] = None

    distrib: None | str

    source: None | str

    #: Stock tag selection.
    tag: None | dict[Literal["with", "without"], Tag | dict | AllOf[Tag] | AnyOf[Tag] | OneOf[Tag]] = None

    #: Documents to load. If a string, should refer to an entry in the
    #: ``alias.t3`` config section. See :ref:`t3-directive-load`.
    load: None | Sequence[str | LoaderDirective] = None

    #: Additional stock filters.
    filter: None | FilterModel = None

    #: Complement stages. See :ref:`t3-directive-complement`.
    complement: None | OneOrMany[str | UnitModel] = None

    #: Units to run. See :ref:`t3-directive-run-execute`.
    run: OneOrMany[str | UnitModel]

    def get_process(self, config: dict[str, Any], logger: AmpelLogger) -> dict[str, Any]:

        d: dict[str, Any] = {
            "include": {
                "session": [
                    {"unit": "T3SessionAlertsNumber"}
                ]
            },
            "execute": [
                {
                    "unit": "T3ReviewUnitExecutor",
                    "config": {
                        "supply": {
                            "unit": "T3DefaultBufferSupplier",
                            "config": {
                                "select": {
                                    "unit": "T3StockSelector",
                                    "config": {
                                        "updated": {
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
                                "load": {
                                    "unit": "T3SimpleDataLoader",
                                    "config": {
                                        "directives": [{"col": col} for col in ("stock", "t0", "t1", "t2")]
                                    }
                                },
                            }
                        },
                        "stage": {
                            "unit": "T3ProjectingStager",
                            "config": {
                                "directives": [
                                    {
                                        "project": {
                                            "unit": "T3ChannelProjector",
                                            "config": {"channel": self.channel}
                                        },
                                        "execute": self.get_units(self.run),
                                    }
                                ]
                            }
                        }
                    }
                }
            ]
        }

        # Restrict stock selection according to T2 values
        if self.filter:
            d["execute"][0]["config"]["supply"]["config"]["select"]["unit"] = "T3FilteringStockSelector"
            d["execute"][0]["config"]["supply"]["config"]["select"]["config"]["t2_filter"] = self.filter.t2.dict()

        if self.channel is None:
            d["execute"][0]["config"]["stage"]["unit"] = "T3SimpleStager"
            del d["execute"][0]["config"]["stage"]["config"]["channel"]
        else:
            # load only documents that pass channel selection
            d["execute"][0]["config"]["supply"]["config"]["load"]["config"]["channel"] = self.channel
   
        # Restrict document types to load
        if self.load:
            d["execute"][0]["config"]["supply"]["config"]["load"]["config"]["directives"] = self.load

        if self.complement:
            d["execute"][0]["config"]["supply"]["config"]["complement"] = self.get_units(self.complement)

        ret: dict[str, Any] = {
            "tier": self.tier,
            "schedule": self.schedule,
            "active": self.active,
            "distrib": self.distrib,
            "source": self.source,
            "channel": self.get_channel_tag(),
            "name": self.name,
            "processor": {
                "unit": "T3Processor",
                "config": d,
            }
        }

        return self._to_dict(ret)


    @classmethod
    def _to_dict(cls, item):
        # TODO: use dictify from ampel.util.mappings ?
        if isinstance(item, dict):
            return {k: cls._to_dict(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [cls._to_dict(v) for v in item]
        elif hasattr(item, "dict"):
            return cls._to_dict(item.dict())
        return item

    def get_units(self, units: OneOrMany[str | UnitModel]) -> list[dict[str, Any]]:
        if isinstance(units, str):
            return [UnitModel(unit=units).dict()]
        elif isinstance(units, UnitModel):
            return [units.dict()]
        return [self.get_units(u)[0] for u in units]

    def get_schedule(self) -> Sequence[str]:
        if isinstance(self.schedule, str):
            return [self.schedule]
        return self.schedule

    def get_channel_tag(self) -> None | str | int:
        """
        Get channel if single channel, otherwise None
        """
        if isinstance(self.channel, str) or isinstance(self.channel, int):
            return self.channel
        return None
