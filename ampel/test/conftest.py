from os import environ
from pathlib import Path

import mongomock
import pytest
from pymongo import InsertOne

from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.ingest.StockIngester import StockIngester
from ampel.ingest.StockT2Ingester import StockT2Ingester
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.model.ingest.T2IngestModel import T2IngestModel
from ampel.test.dummy import DummyCompoundIngester, DummyStateT2Ingester


@pytest.fixture(scope="session")
def mongod():
    if "MONGO_HOSTNAME" in environ and "MONGO_PORT" in environ:
        yield "mongodb://{}:{}".format(environ["MONGO_HOSTNAME"], environ["MONGO_PORT"])
    else:
        pytest.skip("No Mongo instance configured")


@pytest.fixture(scope="session")
def graphite():
    if "GRAPHITE_HOSTNAME" in environ and "GRAPHITE_PORT" in environ:
        yield "graphite://{}:{}".format(
            environ["GRAPHITE_HOSTNAME"], environ["GRAPHITE_PORT"]
        )
    else:
        pytest.skip("No Graphite instance configured")


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.db.AmpelDB.MongoClient", mongomock.MongoClient)
    # ignore codec_options in DBContentLoader
    monkeypatch.setattr("mongomock.codec_options.is_supported", lambda *args: None)


@pytest.fixture
def testing_config():
    return Path(__file__).parent / "test-data" / "testing-config.yaml"


@pytest.fixture
def dev_context(patch_mongo, testing_config):
    config = AmpelConfig.load(testing_config)
    return DevAmpelContext.new(config=config, purge_db=True)


@pytest.fixture
def ampel_logger():
    return AmpelLogger.get_logger()


@pytest.fixture
def ingest_stock(dev_context, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=ampel_logger)
    logd = LogsBufferDict({"logs": [], "extra": {}, "err": False,})
    ingester = StockIngester(
        updates_buffer=updates_buffer, logd=logd, run_id=run_id, context=dev_context,
    )
    ingester.ingest("stockystock", [("TEST_CHANNEL", True)], jextra={"alert": 123})
    ingester.updates_buffer.push_updates()
    assert dev_context.db.get_collection("stock").count_documents({}) == 1


@pytest.fixture
def ingest_stock_t2(dev_context, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=ampel_logger)
    logd = LogsBufferDict({"logs": [], "extra": {}, "err": False,})
    ingester = StockT2Ingester(
        updates_buffer=updates_buffer, logd=logd, run_id=run_id, context=dev_context,
    )
    ingester.add_ingest_models("TEST_CHANNEL", [T2IngestModel(unit="CaptainObvious")])
    ingester.ingest("stockystock", [("TEST_CHANNEL", True)])
    ingester.updates_buffer.push_updates()
    assert dev_context.db.get_collection("t2").count_documents({}) == 1


@pytest.fixture
def ingest_tied_t2(dev_context, ampel_logger):
    """Create a T2 document with dependencies"""
    run_id = 0
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=ampel_logger)
    logd = LogsBufferDict({"logs": [], "extra": {}, "err": False,})
    filter_results = [("TEST_CHANNEL", True)]

    # mimic a datapoint ingester
    datapoints = [
        {"_id": i, "stock": "stockystock", "body": {"thing": i + 1}} for i in range(3)
    ]
    for dp in datapoints:
        updates_buffer.add_t0_update(InsertOne(dp))

    # create compounds
    comp_ingester = DummyCompoundIngester(
        updates_buffer=updates_buffer, logd=logd, run_id=run_id, context=dev_context,
    )
    comp_ingester.add_channel(filter_results[0][0])
    blueprint = comp_ingester.ingest("stockystock", datapoints, filter_results)

    # create T2 doc
    ingester = DummyStateT2Ingester(
        updates_buffer=updates_buffer, logd=logd, run_id=run_id, context=dev_context,
    )
    # FIXME: should dependent docs be created implicitly?
    # NB: docs are inserted in reverse order so that t2 execution is forced to
    # explicitly resolve dependencies
    ingester.add_ingest_models(
        filter_results[0][0],
        [
            T2IngestModel(unit=kind)
            for kind in reversed(("DummyStateT2Unit", "DummyTiedStateT2Unit"))
        ],
    )
    ingester.ingest("stockystock", blueprint, filter_results)
    ingester.updates_buffer.push_updates()

    assert (
        dev_context.db.get_collection("t2").count_documents(
            {"unit": "DummyStateT2Unit"}
        )
        == 1
    )
    assert (
        dev_context.db.get_collection("t2").count_documents(
            {"unit": "DummyTiedStateT2Unit"}
        )
        == 1
    )
