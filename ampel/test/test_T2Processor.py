import pytest
from time import time

from unittest.mock import Mock
from ampel.enum.T2RunState import T2RunState
from ampel.enum.T2SysRunState import T2SysRunState
from ampel.core.AmpelContext import AmpelContext
from contextlib import contextmanager
from pymongo.errors import OperationFailure

from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.t2.T2Processor import T2Processor
from pytest_mock import MockerFixture


@contextmanager
def collect_diff(store):
    store.clear()
    before = {}
    for metric in AmpelMetricsRegistry.registry().collect():
        for sample in metric.samples:
            key = (sample.name, tuple(sample.labels.items()))
            before[key] = sample.value

    delta = {}
    yield
    for metric in AmpelMetricsRegistry.registry().collect():
        for sample in metric.samples:
            key = (sample.name, tuple(sample.labels.items()))
            delta[key] = sample.value - (before.get(key) or 0)
    store.update(delta)


def test_metrics(integration_context, ingest_stock_t2):
    t2 = T2Processor(context=integration_context, raise_exc=True, process_name="t2")

    stats = {}
    with collect_diff(stats):
        assert t2.run() == 1

    assert (
        stats[("ampel_t2_docs_processed_total", (("unit", "DummyStockT2Unit"),))] == 1
    )
    assert stats[("ampel_t2_latency_seconds_sum", (("unit", "DummyStockT2Unit"),))] > 0


def test_error_reporting(integration_context: AmpelContext):
    # DummyPointT2Unit will raise an error on the malformed T0 doc
    integration_context.db.get_collection("t0").insert_one({
        "_id": 42
    })
    channels = ["channel_a", "channel_b"]
    integration_context.db.get_collection("t2").insert_one({
        "unit": "DummyPointT2Unit",
        "status": T2SysRunState.NEW,
        "config": None,
        "col": "t0",
        "stock": 42,
        "link": 42,
        "channel": channels,
    })
    t2 = T2Processor(context=integration_context, raise_exc=False, process_name="t2", run_dependent_t2s=True)
    t2.run()
    assert (doc := integration_context.db.get_collection("t2").find_one({}))
    assert doc["status"] == T2SysRunState.EXCEPTION
    assert (doc := integration_context.db.get_collection("troubles").find_one({}))
    assert doc["doc"]["channel"] == channels

def test_tied_t2s(integration_context, ingest_tied_t2):
    assert (num_dps := integration_context.db.get_collection("t0").count_documents({}))
    t2 = T2Processor(context=integration_context, raise_exc=True, process_name="t2", run_dependent_t2s=True)

    num_docs = t2.run()
    t2 = integration_context.db.get_collection("t2")
    assert t2.count_documents({}) == num_docs

    if "point" in ingest_tied_t2.param.lower():
        assert num_docs == 1+num_dps
        assert t2.find_one({"unit": "DummyPointT2Unit"})["body"][0]["result"]["thing"] == 3
        assert t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["result"]["thing"] == 2*3
    elif "stock" in ingest_tied_t2.param.lower():
        assert num_docs == 2
        assert t2.find_one({"unit": "DummyStockT2Unit"})["body"][0]["result"]["id"] == "stockystock"
        assert t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["result"]["id"] == 2*"stockystock"
    else:
        assert num_docs == 2
        assert t2.find_one({"unit": "DummyStateT2Unit"})["body"][0]["result"]["len"] == num_dps
        assert t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["result"]["len"] == 2*num_dps


def test_slow_dependency(
    integration_context: AmpelContext, ingest_tied_t2, mocker: MockerFixture
):
    """
    Simulate a race conditions between parallel T2 workers
    """
    assert (num_dps := integration_context.db.get_collection("t0").count_documents({}))
    t2 = T2Processor(
        context=integration_context,
        raise_exc=True,
        process_name="t2",
        run_dependent_t2s=True,
        backoff_on_retry={"jitter": False, "factor": 10},
    )

    # num_docs = t2.run()
    col = integration_context.db.get_collection("t2")

    # set upstream docs to RUNNING, simulating the effect of
    # a parallel worker picking them up
    assert (
        col.update_many(
            {"unit": {"$not": {"$regex": ".*Tied.*"}}},
            {"$set": {"status": T2SysRunState.RUNNING}},
        ).modified_count
        > 0
    )

    # should only find depdendent doc once.
    try:
        assert t2.run() == 1
    except OperationFailure as exc:
        if str(exc) == "Unrecognized expression '$last'":
            # see:
            # - https://github.com/mongomock/mongomock/pull/734
            # - https://github.com/mongomock/mongomock/pull/770
            pytest.xfail("mongomock doesn't support $last (yet)")
        else:
            raise
    assert (
        col.count_documents({"status": T2SysRunState.PENDING_DEPENDENCY}) == 1
    ), "exactly 1 dependent doc marked as pending"
    dependent_doc = col.find_one({"status": T2SysRunState.PENDING_DEPENDENCY})
    meta = dependent_doc["body"][-1]
    assert t2.backoff_on_retry is not None
    assert meta["retry_after"] == meta["ts"] + t2.backoff_on_retry.factor
    assert t2.run() == 0, "no more docs to run"

    # run upstream docs
    while (
        doc := col.find_one_and_update(
            {"status": T2SysRunState.RUNNING}, {"$set": {"status": T2SysRunState.NEW}}
        )
    ) is not None:
        assert t2.run() == 1, "new doc run"
        assert (
            col.find_one({"_id": doc["_id"]})["status"] == T2RunState.COMPLETED
        ), "new doc finished"

    assert t2.run() == 0, "no docs to run; dependent still pending"

    # blast AbsWorker one minute into the future, past the retry_after time
    ptime = mocker.patch("ampel.t2.T2Processor.time", return_value=time() + 60)

    assert t2.run() == 1
    assert ptime.called
    assert col.find_one({"_id": dependent_doc["_id"]})["status"] == T2RunState.COMPLETED