#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/test/test_T3Processor.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  25.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.StockAttributes import StockAttributes
from ampel.enum.EventCode import EventCode
from ampel.view.SnapView import SnapView
from ampel.view.T3Store import T3Store
import pytest

from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit, T3Send
from ampel.t3.T3Processor import T3Processor


class Mutineer(AbsT3ReviewUnit):
    raise_on_process: bool = False

    def process(self, views, t3s=None):
        if self.raise_on_process:
            raise ValueError


def mutineer_process(config={}):

    return {
        "execute": [
            {
                "unit": "T3ReviewUnitExecutor",
                "config": {
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
                            "execute": [{"unit": "Mutineer", "config": config}]
                        }
                    }
                }
            }
        ]
    }


@pytest.mark.parametrize(
    "config,expect_success",
    [
        ({}, True),
        ({"raise_on_process": True}, False),
    ]
)
def test_unit_raises_error(
    dev_context: DevAmpelContext, ingest_stock, config, expect_success
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


def test_view_generator(integration_context: DevAmpelContext, ingest_stock):

    class SendySend(AbsT3ReviewUnit):
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
        execute = [
            {
                "unit": "T3ReviewUnitExecutor",
                "config": {
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
                            "execute": [{"unit": "SendySend"}]
                        }
                    }
                }
            }
        ]
    )
    t3.run()

    stock = integration_context.db.get_collection("stock").find_one()
    assert stock
    assert "TAGGYTAG" in stock["tag"]
    assert "floopsy" in stock["name"]
    assert len(entries := [jentry for jentry in stock["journal"] if jentry["tier"] == 3]) == 1
    jentry = entries[0]
    assert jentry["extra"] == {"foo": "bar"}

def test_empty_generator(integration_context: DevAmpelContext, ingest_stock):
    """
    Empty selection returns cleanly, rather than raising
    """ 
    t3 = T3Processor(
        context=integration_context,
        raise_exc=True,
        process_name="t3",
        execute = [
            {
                "unit": "T3ReviewUnitExecutor",
                "config": {
                    "supply": {
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
                    "stage": {
                        "unit": "T3SimpleStager",
                        "config": {
                            "execute": [{"unit": "DemoReviewT3Unit"}]
                        }
                    }
                }
            }
        ]
    )
    t3.run()
