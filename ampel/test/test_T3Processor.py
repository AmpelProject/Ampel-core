from ampel.dev.DevAmpelContext import DevAmpelContext
import pytest

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.t3.T3Processor import T3Processor


class Mutineer(AbsT3Unit):
    raise_on_process: bool = False

    def process(self, views):
        if self.raise_on_process:
            raise ValueError


def mutineer_process(config={}):

    return {
        "process_name": "yarrr",
        "supply": {
            "unit": "T3DefaultSupplier",
            "config": {
                "process_name": "yarrr",
                "select": {
                    "unit": "T3StockSelector",
                },
                "load": {
                    "unit": "T3SimpleDataLoader",
                    "config": {
                        "directives": [
                            {"col": "stock"},
                        ]
                    },
                },
            },
        },
        "stage": {
            "unit": "T3SimpleStager",
            "config": {
                "raise_exc": True,
                "execute": [{"unit": "Mutineer", "config": config}],
            },
        },
    }


@pytest.mark.parametrize(
    "config,expect_success",
    [
        ({}, True),
        ({"raise_on_process": True}, False),
    ],
)
def test_unit_raises_error(dev_context: DevAmpelContext, ingest_stock, config, expect_success):
    """Run is marked failed if units raise an exception"""
    dev_context.register_unit(Mutineer)
    t3 = T3Processor(context=dev_context, raise_exc=False, **mutineer_process(config))
    t3.run()
    assert dev_context.db.get_collection("events").count_documents({}) == 1
    event = dev_context.db.get_collection("events").find_one({})
    assert event["run"] == 1
    assert event["success"] == expect_success
