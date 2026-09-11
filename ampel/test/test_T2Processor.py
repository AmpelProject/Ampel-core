from collections.abc import Iterable
from contextlib import contextmanager
from time import time
from typing import Any

from mongomock import ObjectId
import pytest
from pymongo.errors import OperationFailure
from pytest_mock import MockerFixture

from ampel.content.T2Document import T2Document
from ampel.core.AmpelContext import AmpelContext
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.enum.DocumentCode import DocumentCode
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.UnitModel import UnitModel
from ampel.queue.QueueIngester import AbsProducer, QueueIngester
from ampel.t2.T2QueueWorker import AbsConsumer, QueueItem, T2QueueWorker
from ampel.ingest.IngestionWorker import IngestionWorker
from ampel.t2.T2Worker import T2Worker
from ampel.test.conftest import make_tied_ingestion_handler
from ampel.test.dummy import DummyPointT2Unit


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


@pytest.mark.usefixtures("_ingest_stock_t2")
def test_metrics(integration_context):
    t2 = T2Worker(context=integration_context, raise_exc=True, process_name="t2")

    stats = {}
    with collect_diff(stats):
        assert t2.run() == 1

    assert (
        stats[("ampel_t2_docs_processed_total", (("unit", "DummyStockT2Unit"), ('code', 'OK')))] == 1
    )
    assert stats[("ampel_t2_latency_seconds_sum", (("unit", "DummyStockT2Unit"),))] > 0


@pytest.mark.parametrize("config", [None, {"raise_exc": True}])
def test_error_reporting(integration_context: DevAmpelContext, config):
    integration_context.register_unit(DummyPointT2Unit)
    # DummyPointT2Unit will raise an error on the malformed T0 doc
    integration_context.db.get_collection("t0").insert_one({"id": 42, "stock": 42})
    channels = ["channel_a", "channel_b"]
    doc: T2Document = {
        "unit": "DummyPointT2Unit",
        "code": DocumentCode.NEW,
        "config": config,
        "col": "t0",
        "stock": 42,
        "link": 42,
        "channel": channels,
        "meta": [
            {"ts": 0, "tier": 2}
        ],
        "body": []
    }
    integration_context.db.get_collection("t2").insert_one(doc)
    t2 = T2Worker(context=integration_context, raise_exc=False, process_name="t2", run_dependent_t2s=True)
    assert t2.run() == 1
    assert (doc := integration_context.db.get_collection("t2").find_one({})) # type: ignore[assignment]
    assert doc["code"] == DocumentCode.EXCEPTION
    assert (trouble := integration_context.db.get_collection('trouble').find_one({}))
    if config is None:
        assert trouble["channel"] == channels
        assert trouble.get("msg") is None
    else:
        assert trouble["extra"]["msg"] == "Could not instantiate unit"


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
    assert integration_context.db.get_collection("t0").count_documents({})
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


@pytest.mark.parametrize("ingester_impl", ["mongo", "queue"])
def test_queue_worker(
    mock_context: DevAmpelContext, mocker: MockerFixture, ampel_logger, ingester_impl: str
):
    """
    Simulate a race conditions between parallel T2 workers
    """

    queues: dict[str, list[AbsProducer.Item]] = {
        "t2": [],
        "ingest": []
    }

    @mock_context.register_unit
    class DummyProducer(AbsProducer):

        channel: str

        def produce(self, item: AbsProducer.Item, delivery_callback=None):
            queues[self.channel].append(item)
            if delivery_callback:
                delivery_callback()

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    @mock_context.register_unit
    class DummyConsumer(AbsConsumer):

        channel: str

        def consume(self) -> None | QueueItem:
            if not queues[self.channel]:
                return None
            item = queues[self.channel].pop()
            return {"stock": item.stock, "t0": item.t0, "t1": item.t1, "t2": item.t2}

        def acknowledge(self, docs: Iterable[QueueItem]) -> None:
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    ack = mocker.patch.object(DummyConsumer, "acknowledge")

    mock_context.register_unit(QueueIngester)

    handler = make_tied_ingestion_handler(
        mock_context,
        ampel_logger,
        "DummyStateT2Unit",
        UnitModel(
            unit="QueueIngester",
            config={"producer": {"unit": "DummyProducer", "config": {"channel": "t2"}}},
        ),
    )

    datapoints: list[dict[str, Any]] = [
        {"id": i, "stock": "stockystock", "body": {"thing": i + 1}} for i in range(3)
    ]

    with handler.ingester.group():
        handler.ingest(datapoints, [(0, True)], stock_id="stockystock", jm_extra={"alert": 123})

    t2 = T2QueueWorker(
        context=mock_context,
        consumer={"unit": "DummyConsumer", "config": {"channel": "t2"}},
        ingester={"unit": "QueueIngester", "config": {"producer": {"unit": "DummyProducer", "config": {"channel": "ingest"}}}} if ingester_impl == "queue" else {"unit": "MongoIngester"},
        raise_exc=True,
        process_name="t2",
        run_dependent_t2s=True,
        backoff_on_retry=[{"jitter": False, "factor": 10}],
    )

    assert t2.run() == 1

    assert ack.call_count == 1
    acks_iter = ack.call_args[0][0]
    assert isinstance(acks_iter, Iterable)
    acks = list(acks_iter)
    assert acks
    assert acks[0]["stock"][0]["stock"] == "stockystock"

    if ingester_impl == "queue":
        assert len(queues["ingest"]) == 1, "ingest queue should have been populated"

        ingest = IngestionWorker(
            context=mock_context,
            process_name="ingest",
            consumer={"unit": "DummyConsumer", "config": {"channel": "ingest"}},
        )

        assert ingest.run() == 1

    assert mock_context.db.get_collection("stock").count_documents({}) == 1
    assert mock_context.db.get_collection("t0").count_documents({}) == 3
    assert mock_context.db.get_collection("t1").count_documents({}) == 1
    t1_doc = mock_context.db.get_collection("t1").find_one({})
    assert t1_doc is not None
    assert len(t1_doc["meta"]) == 1

    docs = list(mock_context.db.get_collection("t2").find({"code": DocumentCode.OK}))
    assert len(docs) == 2
    assert len(docs[0]["body"]) == 1
    assert len(docs[0]["meta"]) == 2

    with handler.ingester.group():
        handler.ingest(datapoints, [(0, True)], stock_id="stockystock", jm_extra={"alert": 456})
    
    assert t2.run() == 1

    if ingester_impl == "queue":
        assert len(queues["ingest"]) == 1, "ingest queue should have been populated"

        assert ingest.run() == 1

    assert mock_context.db.get_collection("t1").count_documents({}) == 1
    t1_doc = mock_context.db.get_collection("t1").find_one({})
    assert t1_doc is not None
    assert len(t1_doc["meta"]) == 2, "meta entry added to t1 doc"
    docs = list(mock_context.db.get_collection("t2").find({"code": DocumentCode.OK}))
    assert len(docs) == 2
    assert len(docs[0]["body"]) == 1, "doc was not re-run"
    assert len(docs[0]["meta"]) == 3, "meta entry added to t2 doc"

    stock_doc = mock_context.db.get_collection("stock").find_one({})
    assert stock_doc is not None
    assert "ts" in stock_doc
    for k in stock_doc["ts"]:
        assert "tied" in stock_doc["ts"][k]
        assert "upd" in stock_doc["ts"][k]
        assert stock_doc["ts"][k]["upd"] >= stock_doc["ts"][k]["tied"]
    assert "tied" in stock_doc["ts"]["any"]
    t2_journal_entries = [j for j in stock_doc.get("journal", []) if j.get("tier") == 2]
    assert len(t2_journal_entries) == 2, "t2 journal entry added to stock doc"

    for journal in t2_journal_entries:
        assert "doc" in journal
        if isinstance(journal["doc"], bytes):
            query = {"_id": ObjectId(journal["doc"])}
        else:
            query = journal["doc"]
        t2_doc = mock_context.db.get_collection("t2").find_one(query)
        assert t2_doc is not None
        assert t2_doc["code"] == DocumentCode.OK
        assert t2_doc["unit"] == journal["unit"]