from os import environ
from pathlib import Path

import mongomock
import pytest

from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger


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
