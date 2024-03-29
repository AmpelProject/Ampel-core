#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/test/test_T3Processor.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  25.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator
from typing import Any

import pytest

from ampel.abstract.AbsT3Unit import AbsT3Unit, T3Send
from ampel.content.StockDocument import StockDocument
from ampel.content.T2Document import T2Document
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.EventCode import EventCode
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.StockAttributes import StockAttributes
from ampel.struct.T3Store import T3Store
from ampel.t3.T3Processor import T3Processor
from ampel.test.dummy import DummyStateT2Unit
from ampel.util.config import get_unit_confid
from ampel.view.SnapView import SnapView


class Mutineer(AbsT3Unit):
    raise_on_process: bool = False

    def process(self, views, t3s=None):
        if self.raise_on_process:
            raise ValueError


def mutineer_process(config=None):

    return {
        "supply": {
            "unit": "T3DefaultBufferSupplier",
            "config": {
                "select": {"unit": "T3StockSelector"},
                "load": {
                    "unit": "T3SimpleDataLoader",
                    "config": {
                        "directives": [{"col": "stock"}]
                    }
                }
            }
        },
        "stage": {
            "unit": "T3SimpleStager",
            "config": {
                "execute": [{"unit": "Mutineer", "config": config or {}}]
            }
        }
    }


@pytest.mark.usefixtures("_ingest_stock")
@pytest.mark.parametrize(
    ("config","expect_success"),
    [
        ({}, True),
        ({"raise_on_process": True}, False),
    ]
)
def test_unit_raises_error(
    dev_context: DevAmpelContext, config, expect_success
):
    """Run is marked failed if units raise an exception"""
    dev_context.register_unit(Mutineer)
    t3 = T3Processor(context=dev_context, process_name="test", raise_exc=False, **mutineer_process(config))
    t3.run()
    assert dev_context.db.get_collection('event').count_documents({}) == 1
    event = dev_context.db.get_collection('event').find_one({})
    assert event
    assert event["run"] == 1
    assert event["code"] == EventCode.OK.value if expect_success else EventCode.EXCEPTION


@pytest.mark.usefixtures("_ingest_stock")
def test_view_generator(integration_context: DevAmpelContext):

    class SendySend(AbsT3Unit):
        raise_on_process: bool = False

        def process(self, gen: Generator[SnapView, T3Send, None], t3s: None | T3Store = None):
            for view in gen:
                gen.send(
                    (
                        view.id,
                        StockAttributes(
                            tag="TAGGYTAG",
                            name="floopsy",
                            journal=JournalAttributes(extra={"foo": "bar"}),
                        ),
                    )
                )

    integration_context.register_unit(SendySend)

    t3 = T3Processor(
        context=integration_context,
        raise_exc=True,
        process_name="t3",
        supply = {
            "unit": "T3DefaultBufferSupplier",
            "config": {
                "select": {"unit": "T3StockSelector"},
                "load": {
                    "unit": "T3SimpleDataLoader",
                    "config": {
                        "directives": [{"col": "stock"}]
                    }
                }
            }
        },
        stage = {
            "unit": "T3SimpleStager",
            "config": {
                "execute": [{"unit": "SendySend"}]
            }
        }
    )
    t3.run()

    stock = integration_context.db.get_collection("stock").find_one()
    assert stock
    assert "TAGGYTAG" in stock["tag"]
    assert "floopsy" in stock["name"]
    assert len(entries := [jentry for jentry in stock["journal"] if jentry["tier"] == 3]) == 1
    jentry = entries[0]
    assert jentry["extra"] == {"foo": "bar"}

@pytest.mark.usefixtures("_ingest_stock")
def test_empty_generator(integration_context: DevAmpelContext):
    """
    Empty selection returns cleanly, rather than raising
    """
    t3 = T3Processor(
        context=integration_context,
        raise_exc=True,
        process_name="t3",
        supply = {
            "unit": "T3DefaultBufferSupplier",
            "config": {
                "select": {
                    "unit": "T3StockSelector",
                    # ensure that no stocks will be selected
                    "config": {"channel": "nonesuch"}
                },
                "load": {
                    "unit": "T3SimpleDataLoader",
                    "config": {
                        "directives": [{"col": "stock"}],
                    }
                }
            }
        },
        stage = {
            "unit": "T3SimpleStager",
            "config": {
                "execute": [{"unit": "DemoT3Unit"}]
            }
        }
    )
    t3.run()


class ViewExaminer(AbsT3Unit):
    class DidAThing(Exception):
        ...

    expected_config: dict[str, Any]

    def process(self, views, t3s=None):
        for view in views:
            for t2 in view.t2:
                assert t2.config == self.expected_config
                raise self.DidAThing


@pytest.mark.parametrize("num_units", [1, 2])
def test_t2_config_resolution(mock_context: DevAmpelContext, num_units: int):
    """ T3 stagers pass through resolved config ids """

    for unit in (ViewExaminer, DummyStateT2Unit):
        mock_context.register_unit(unit)

    expected_config = {"foo": 42}
    stock: StockDocument = {"stock": 0}
    confid = get_unit_confid(mock_context.loader, "DummyStateT2Unit", expected_config)
    mock_context.add_conf_id(confid, expected_config)
    t2: T2Document = {
        "unit": "DummyStateT2Unit",
        "config": confid,
        "stock": 0,
        "link": 0,
        "channel": "TEST",
        "meta": [],
        "body": [],
        "code": DocumentCode.OK,
    }
    mock_context.db.get_collection("stock").insert_one(dict(stock))
    mock_context.db.get_collection("t2").insert_one(dict(t2))

    t3 = T3Processor(
        context=mock_context,
        raise_exc=True,
        process_name="t3",
        supply = {
            "unit": "T3DefaultBufferSupplier",
            "config": {
                "select": {
                    "unit": "T3StockSelector",
                },
                "load": {
                    "unit": "T3SimpleDataLoader",
                    "config": {
                        "directives": [{"col": "t2"}],
                    }
                }
            }
        },
        stage = {
            "unit": "T3SimpleStager",
            "config": {
                "execute": [
                    {
                        "unit": "ViewExaminer",
                        "config": {"expected_config": expected_config},
                    }
                ]
                * num_units
            }
        }
    )
    with pytest.raises(ViewExaminer.DidAThing):
        t3.run()
