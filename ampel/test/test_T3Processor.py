import pytest

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.t3.T3Processor import T3Processor


class Mutineer(AbsT3Unit):
    raise_on_add: bool = False
    raise_on_done: bool = False

    def add(self, views):
        if self.raise_on_add:
            raise ValueError

    def done(self):
        if self.raise_on_done:
            raise ValueError


def mutineer_process(config={}):
    return {
        "process_name": "yarrr",
        "chunk_size": 1,
        "directives": [
            {
                "select": {"unit": "T3StockSelector"},
                "load": {
                    "unit": "T3SimpleDataLoader",
                    "config": {"directives": [{"col": "stock"},]},
                },
                "run": {
                    "unit": "T3UnitRunner",
                    "config": {
                        "raise_exc": True,
                        "directives": [
                            {"execute": [{"unit": Mutineer, "config": config}]}
                        ],
                    },
                },
            }
        ],
    }


@pytest.mark.parametrize(
    "config,expect_success",
    [({}, True), ({"raise_on_done": True}, False), ({"raise_on_add": True}, False),],
)
def test_unit_raises_error(dev_context, ingest_stock, config, expect_success):
    """Run is marked failed if units raise an exception"""
    t3 = T3Processor(context=dev_context, raise_exc=False, **mutineer_process(config))
    t3.run()
    assert dev_context.db.get_collection("events").count_documents({}) == 1
    event = dev_context.db.get_collection("events").find_one({})
    assert event["run"] == 1
    assert event["success"] == expect_success
