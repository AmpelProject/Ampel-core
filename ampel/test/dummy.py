#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/test/dummy.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 11.02.2021
# Last Modified By  : jvs

import time
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union, Any

from pymongo import UpdateOne

from ampel.struct.UnitResult import UnitResult
from ampel.types import ChannelId, StockId
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsTiedStateT2Unit import AbsTiedStateT2Unit
from ampel.abstract.ingest.AbsT1Ingester import AbsT1Ingester
from ampel.abstract.ingest.AbsStateT2Compiler import AbsStateT2Compiler
from ampel.abstract.ingest.AbsStateT2Ingester import AbsStateT2Ingester
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester
from ampel.content.DataPoint import DataPoint
from ampel.ingest.T1Compiler import T1Compiler
from ampel.t1.T1SimpleCombiner import T1SimpleCombiner
from ampel.log.AmpelLogger import AmpelLogger
from ampel.enum.DocumentCode import DocumentCode
from ampel.content.T2Document import T2Document
from ampel.content.DataPoint import DataPoint
from ampel.content.T1Document import T1Document
from ampel.view.T2DocView import T2DocView
from ampel.model.StateT2Dependency import StateT2Dependency


class Sleepy(AbsEventUnit):
    """
    A processor that does nothing (especially not touching the db, which is not
    mocked in subprocesses)
    """

    def run(self):
        time.sleep(1)


class DummyStockT2Unit(AbsStockT2Unit):
    def process(self, stock_doc):
        return {"id": stock_doc["_id"]}


# FIXME: these dummy ingesters are copied from ampel-alerts, as there is no
# default implementation of AbsT1Ingester or AbsStateT2Ingester
class DummyCompoundIngester(AbsT1Ingester):
    """simplified PhotoT1Ingester for testing"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.channels: Set[ChannelId] = set()
        self.engine = T1SimpleCombiner(logger=AmpelLogger.get_logger(console=False))

    def add_channel(self, channel: ChannelId) -> None:
        self.channels.add(channel)

    def ingest(
        self,
        stock_id: StockId,
        datapoints: Sequence[DataPoint],
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
    ) -> Optional[T1Compiler]:
        chans = [k for k, v in chan_selection if k in self.channels]

        if not (blue_print := self.engine.combine(stock_id, datapoints, chans)):
            return None

        for eff_comp_id in blue_print.get_effids_for_chans(chans):

            d_addtoset = {
                "channel": {
                    "$each": list(blue_print.get_chans_with_effid(eff_comp_id))
                },
                "run": self.run_id,
            }

            if blue_print.has_flavors(eff_comp_id):
                d_addtoset["flavor"] = {
                    "$each": blue_print.get_compound_flavors(eff_comp_id)
                }

            comp_dict = blue_print.get_eff_compound(eff_comp_id)

            comp_set_on_ins = {
                "_id": eff_comp_id,
                "stock": stock_id,
                "tag": list(blue_print.get_doc_tags(eff_comp_id)),
                "tier": 0,
                "added": time.time(),
                "len": len(comp_dict),
                "body": comp_dict,
            }

            self.updates_buffer.add_t1_update(
                UpdateOne(
                    {"_id": eff_comp_id},
                    {"$setOnInsert": comp_set_on_ins, "$addToSet": d_addtoset},
                    upsert=True,
                )
            )

        return blue_print


class DummyStateT2Compiler(AbsStateT2Compiler):
    def compile(
        self,
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
        compound_blueprint: T1Compiler,
    ) -> Dict[
        Tuple[str, Optional[int], Union[bytes, Tuple[bytes, ...]]], Set[ChannelId]
    ]:
        t2s_for_channels = defaultdict(set)
        for chan, ingest_model in self.get_ingest_models(chan_selection):
            t2s_for_channels[(ingest_model.unit_id, ingest_model.config)].add(chan)

        optimized_t2s: Dict[
            Tuple[str, Optional[int], Union[bytes, Tuple[bytes, ...]]], Set[ChannelId]
        ] = {}
        for k, v in t2s_for_channels.items():
            comp_ids = tuple(compound_blueprint.get_effids_for_chans(v))
            if len(comp_ids) == 1:
                optimized_t2s[k + (comp_ids[0],)] = v
            else:
                optimized_t2s[k + (comp_ids,)] = v
        return optimized_t2s


class DummyStateT2Ingester(AbsStateT2Ingester):
    compiler: AbsStateT2Compiler[T1Compiler] = DummyStateT2Compiler()

    def ingest(
        self,
        stock_id: StockId,
        comp_bp: T1Compiler,
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
    ) -> None:
        """
        Simplified version of PhotoT2Ingester, with t2 docs linked to exactly
        one compound
        """
        optimized_t2s = self.compiler.compile(chan_selection, comp_bp)
        now = int(time.time())

        # Loop over t2 units to be created
        for (t2_id, run_config, link_id), chans in optimized_t2s.items():

            # Matching search criteria
            match_dict: Dict[str, Any] = {
                "stock": stock_id,
                "unit": t2_id,
                "config": run_config
                # 'link' is added below
            }

            # Attributes set if no previous doc exists
            set_on_insert: T2Document = {
                "stock": stock_id,
                "unit": t2_id,
                "config": run_config,
                "code": DocumentCode.NEW.value,
            }
            if self.tags:
                set_on_insert["tag"] = self.tags

            jchan, chan_add_to_set = AbsT2Ingester.build_query_parts(chans)
            add_to_set: Dict[str, Any] = {"channel": chan_add_to_set}

            assert isinstance(link_id, bytes)

            match_dict["link"] = {"$elemMatch": {"$eq": link_id}}
            add_to_set["link"] = link_id

            # Update journal
            add_to_set["journal"] = {"tier": self.tier, "dt": now, "channel": jchan}

            # Append update operation to bulk list
            self.updates_buffer.add_t2_update(
                UpdateOne(
                    match_dict,
                    {"$setOnInsert": set_on_insert, "$addToSet": add_to_set},
                    upsert=True,
                )
            )


class DummyPointT2Unit(AbsPointT2Unit):
    def process(self, datapoint):
        return {"thing": datapoint["body"]["thing"]}


class DummyStateT2Unit(AbsStateT2Unit):
    def process(self, compound, datapoints):
        return {"len": len(datapoints)}


class DummyTiedStateT2Unit(AbsTiedStateT2Unit):

    t2_dependency = [StateT2Dependency(unit="DummyStateT2Unit")]
    _unit = "DummyStateT2Unit"

    @classmethod
    def get_tied_unit_names(cls):
        return [cls._unit]

    def process(self,
		compound: T1Document,
		datapoints: Sequence[DataPoint],
		t2views: Sequence[T2DocView]
	) -> Union[UBson, UnitResult]:
        assert t2views, "dependencies were found"
        assert len(body := t2views[0].body or []) == 1
        return {k: v * 2 for k, v in (t2views[0].get_data() or {}).items()}
