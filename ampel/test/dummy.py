#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/test/dummy.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  11.02.2021
# Last Modified By:    jvs

import time
from collections.abc import Sequence

from ampel.struct.UnitResult import UnitResult
from ampel.types import StockId, UBson
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsTiedStateT2Unit import AbsTiedStateT2Unit

from ampel.content.DataPoint import DataPoint
from ampel.content.T1Document import T1Document
from ampel.view.T2DocView import T2DocView
from ampel.model.StateT2Dependency import StateT2Dependency
from ampel.abstract.AbsT0Muxer import AbsT0Muxer


class Sleepy(AbsEventUnit):
    """
    A processor that does nothing (especially not touching the db, which is not
    mocked in subprocesses)
    """

    process_name: str = "ZZZzzzz"

    def run(self):
        time.sleep(1)


class DummyMuxer(AbsT0Muxer):

    #: number of points to add to provided list
    points_to_insert: int = 5

    def process(
        self, dps: list[DataPoint], stock_id: None | StockId = None
    ) -> tuple[None | list[DataPoint], None | list[DataPoint]]:
        """
        :returns: tuple[datapoints to insert, datapoints to combine]
            <datapoints to insert> will be provided to a T0 ingester
            <datapoints to combine> will potentially be provided to an underlying T1 combiner
        """

        new_dps: list[DataPoint] = [
            {"id": i, "stock": stock_id or 0}
            for i in range(dps[-1]["id"] + 1, dps[-1]["id"] + 1 + self.points_to_insert)
        ]
        assert self.points_to_insert == 5
        assert len(new_dps) == self.points_to_insert
        return new_dps + dps, new_dps + dps


class DummyStockT2Unit(AbsStockT2Unit):
    def process(self, stock_doc):
        return {"id": stock_doc["stock"]}


class DummyPointT2Unit(AbsPointT2Unit):
    def process(self, datapoint):
        return {"thing": datapoint["body"]["thing"]}


class DummyStateT2Unit(AbsStateT2Unit):

    foo: int = 42

    def process(self, compound, datapoints):
        return {"len": len(datapoints)}


class DummyTiedStateT2Unit(AbsTiedStateT2Unit):

    t2_dependency = [StateT2Dependency(unit="DummyStateT2Unit")] # type: ignore[var-annotated]

    def process(
        self,
        compound: T1Document,
        datapoints: Sequence[DataPoint],
        t2views: Sequence[T2DocView],
    ) -> UBson | UnitResult:
        assert t2views, "dependencies were found"
        assert len(t2views[-1].body or []) == 1
        data = t2views[-1].get_payload() or {}
        assert isinstance(data, dict)
        return {k: v * 2 for k, v in data.items()}
