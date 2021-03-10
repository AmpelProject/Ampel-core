import logging
import subprocess

import pytest
import mongomock

from ampel.t3.select.T3FilteringStockSelector import T3FilteringStockSelector
from ampel.t2.T2Processor import T2Processor
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.config.AmpelConfig import AmpelConfig




@pytest.fixture
def processed_t2s(dev_context, ingest_tied_t2):
    assert (num_dps := dev_context.db.get_collection("t0").count_documents({}))
    t2 = T2Processor(
        context=dev_context, raise_exc=True, process_name="t2", run_dependent_t2s=True
    )

    num_docs = t2.run()
    t2 = dev_context.db.get_collection("t2")
    assert t2.count_documents({}) == num_docs

    if "point" in ingest_tied_t2.param.lower():
        assert num_docs == 1 + num_dps
        assert (
            t2.find_one({"unit": "DummyPointT2Unit"})["body"][0]["result"]["thing"] == 3
        )
        assert (
            t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["result"]["thing"]
            == 2 * 3
        )
    elif "stock" in ingest_tied_t2.param.lower():
        assert num_docs == 2
        assert (
            t2.find_one({"unit": "DummyStockT2Unit"})["body"][0]["result"]["id"]
            == "stockystock"
        )
        assert (
            t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["result"]["id"]
            == 2 * "stockystock"
        )
    else:
        assert num_docs == 2
        assert (
            t2.find_one({"unit": "DummyStateT2Unit"})["body"][0]["result"]["len"]
            == num_dps
        )
        assert (
            t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["result"]["len"]
            == 2 * num_dps
        )

    return ingest_tied_t2.param


def test_filter(dev_context, processed_t2s, ampel_logger):
    if isinstance(dev_context.db.get_collection("t2").database.client, mongomock.MongoClient):
        pytest.skip("T3FilteringStockSelector does not work with mongomock")
    if processed_t2s != "DummyStateT2Unit":
        pytest.skip("Only deal with a single unit for now")
    selector = T3FilteringStockSelector(
        context=dev_context,
        logger=ampel_logger,
        t2_filter={"unit": processed_t2s, "match": {"len": {"$gt": 1}}},
    )

    assert len(ids := list(selector.fetch())) == 1
    assert ids[0] == {"_id": "stockystock"}
