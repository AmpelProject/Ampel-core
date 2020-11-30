from os import environ
from pathlib import Path

import mongomock
import pytest

from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.ingest.StockIngester import StockIngester
from ampel.ingest.StockT2Ingester import StockT2Ingester
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.model.ingest.T2IngestModel import T2IngestModel


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
def dev_context(patch_mongo):
    config = AmpelConfig.load(
        Path(__file__).parent / "test-data" / "testing-config.yaml",
    )
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
