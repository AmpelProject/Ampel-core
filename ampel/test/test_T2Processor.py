import pytest
from time import time

from ampel.content.T2Document import T2Document
from ampel.core.AmpelContext import AmpelContext
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.enum.DocumentCode import DocumentCode
from contextlib import contextmanager
from pymongo.errors import OperationFailure

from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.t2.T2Worker import T2Worker
from ampel.test.dummy import DummyPointT2Unit
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
    t2 = T2Worker(context=integration_context, raise_exc=True, process_name="t2")

    stats = {}
    with collect_diff(stats):
        assert t2.run() == 1

    assert (
        stats[("ampel_t2_docs_processed_total", (("unit", "DummyStockT2Unit"),))] == 1
    )
    assert stats[("ampel_t2_latency_seconds_sum", (("unit", "DummyStockT2Unit"),))] > 0


def test_error_reporting(integration_context: DevAmpelContext):
    integration_context.register_unit(DummyPointT2Unit)
    # DummyPointT2Unit will raise an error on the malformed T0 doc
    integration_context.db.get_collection("t0").insert_one({"id": 42})
    channels = ["channel_a", "channel_b"]
    doc: T2Document = {
        "unit": "DummyPointT2Unit",
        "code": DocumentCode.NEW,
        "config": None,
        "col": "t0",
        "stock": 42,
        "link": 42,
        "channel": channels,
        "meta": [
            {"ts": 0, "tier": 2}
        ],
        "body": []
    }
    integration_context.db.get_collection("t2").insert_one(doc) # type: ignore[arg-type]
    t2 = T2Worker(context=integration_context, raise_exc=False, process_name="t2", run_dependent_t2s=True)
    assert t2.run() == 1
    assert (doc := integration_context.db.get_collection("t2").find_one({})) # type: ignore[assignment]
    assert doc["code"] == DocumentCode.EXCEPTION
    assert (trouble := integration_context.db.get_collection('trouble').find_one({}))
    assert trouble["channel"] == channels


def test_tied_t2s(integration_context, ingest_tied_t2):
    assert (num_dps := integration_context.db.get_collection("t0").count_documents({}))
    t2 = T2Worker(
        context=integration_context, raise_exc=True, process_name="t2", run_dependent_t2s=True
    )

    num_docs = t2.run()
    t2 = integration_context.db.get_collection("t2")
    assert t2.count_documents({}) == num_docs

    if "point" in ingest_tied_t2.param.lower():
        assert num_docs == 1 + num_dps
        assert (
            next(t2.find({"unit": "DummyPointT2Unit"}).sort([("link", -1)]).limit(1))[
                "body"
            ][0]["thing"]
            == 3
        )
        assert (
            next(
                t2.find({"unit": "DummyTiedStateT2Unit"}).sort([("link", -1)]).limit(1)
            )["body"][0]["thing"]
            == 2 * 3
        )
    elif "stock" in ingest_tied_t2.param.lower():
        assert num_docs == 2
        assert (
            t2.find_one({"unit": "DummyStockT2Unit"})["body"][0]["id"] == "stockystock"
        )
        assert (
            t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["id"]
            == 2 * "stockystock"
        )
    else:
        assert num_docs == 2
        assert t2.find_one({"unit": "DummyStateT2Unit"})["body"][0]["len"] == num_dps
        assert (
            t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["len"]
            == 2 * num_dps
        )


def test_slow_dependency(
    integration_context: AmpelContext, ingest_tied_t2, mocker: MockerFixture
):
    """
    Simulate a race conditions between parallel T2 workers
    """
    assert (num_dps := integration_context.db.get_collection("t0").count_documents({}))
    t2 = T2Worker(
        context=integration_context,
        raise_exc=True,
        process_name="t2",
        run_dependent_t2s=True,
        backoff_on_retry=[{"jitter": False, "factor": 10}],
    )

    # num_docs = t2.run()
    col = integration_context.db.get_collection("t2")

    # set upstream docs to RUNNING, simulating the effect of
    # a parallel worker picking them up
    assert (
        col.update_many(
            {"unit": {"$not": {"$regex": ".*Tied.*"}}},
            {"$set": {"code": DocumentCode.RUNNING}},
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
        col.count_documents({"code": DocumentCode.T2_PENDING_DEPENDENCY}) == 1
    ), "exactly 1 dependent doc marked as pending"
    dependent_doc = col.find_one({"code": DocumentCode.T2_PENDING_DEPENDENCY})
    assert dependent_doc is not None
    meta = dependent_doc["meta"][-1]
    assert t2.backoff_on_retry is not None
    assert meta["retry_after"] == meta["ts"] + t2.backoff_on_retry[0].factor
    assert t2.run() == 0, "no more docs to run"

    # run upstream docs
    while (
        doc := col.find_one_and_update(
            {"code": DocumentCode.RUNNING}, {"$set": {"code": DocumentCode.NEW}}
        )
    ) is not None:
        assert t2.run() == 1, "new doc run"
        assert (db_doc := col.find_one({"_id": doc["_id"]})) is not None, "doc found"
        assert (
            db_doc["code"] == DocumentCode.OK
        ), "new doc finished"

    assert t2.run() == 0, "no docs to run; dependent still pending"

    # blast AbsWorker one minute into the future, past the retry_after time
    ptime = mocker.patch("ampel.abstract.AbsWorker.time", return_value=time() + 60)

    assert t2.run() == 1
    assert ptime.called
    assert (db_doc := col.find_one({"_id": dependent_doc["_id"]})) is not None, "dependent doc found"
    assert db_doc["code"] == DocumentCode.OK
