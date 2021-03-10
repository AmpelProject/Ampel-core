from os import environ
from pathlib import Path

import mongomock
import pytest
from pymongo import InsertOne

from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.ingest.PointT2Ingester import PointT2Ingester
from ampel.ingest.StockIngester import StockIngester
from ampel.ingest.StockT2Ingester import StockT2Ingester
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.model.ingest.T2IngestModel import T2IngestModel
from ampel.model.StateT2Dependency import StateT2Dependency
from ampel.test.dummy import DummyCompoundIngester, DummyStateT2Ingester

def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run docker-based integration tests",
    )

@pytest.fixture(scope="session")
def mongod(pytestconfig):
    if "MONGO_PORT" in environ:
        yield "mongodb://localhost:{}".format(environ["MONGO_PORT"])
        return

    if not pytestconfig.getoption("--integration"):
        raise pytest.skip("integration tests require --integration flag")
    try:
        container = subprocess.check_output(
            "docker", "run", "--rm", "-d", "-P", "mongo:4.4"
        )
    except FileNotFoundError:
        pytest.skip("integration tests require docker")
    try:
        port = json.loads(subprocess.check_output("docker", "inspect", container))[0][
            "NetworkSettings"
        ]["Ports"]["27017/tcp"][0]["HostPort"]
        yield f"mongodb://localhost:{port}"
    finally:
        subprocess.check_call("docker", "stop", "container")


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.db.AmpelDB.MongoClient", mongomock.MongoClient)
    # ignore codec_options in DBContentLoader
    monkeypatch.setattr("mongomock.codec_options.is_supported", lambda *args: None)


@pytest.fixture
def testing_config():
    return Path(__file__).parent / "test-data" / "testing-config.yaml"


@pytest.fixture(scope="session")
def core_config():
    """
    The config distributed with ampel-core
    """
    cb = DistConfigBuilder()
    for ext in "json", "yml", "yaml":
        cb.load_distrib("ampel-core", "conf", ext)
    return cb.build_config(config_validator=None)


@pytest.fixture
def mock_context(patch_mongo, testing_config):
    config = AmpelConfig.load(testing_config)
    return DevAmpelContext.new(config=config, purge_db=True)

@pytest.fixture
def integration_context(mongod, testing_config):
    config = AmpelConfig.load(testing_config)
    return DevAmpelContext.new(config=config, purge_db=True, custom_conf={"resource.mongo": mongod})

# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_context", "integration_context"])
def dev_context(request):
    yield request.getfixturevalue(request.param)

@pytest.fixture
def ampel_logger():
    return AmpelLogger.get_logger()


@pytest.fixture
def ingest_stock(dev_context, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=ampel_logger)
    logd = LogsBufferDict(
        {
            "logs": [],
            "extra": {},
            "err": False,
        }
    )
    ingester = StockIngester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    )
    ingester.ingest("stockystock", [("TEST_CHANNEL", True)], jextra={"alert": 123})
    ingester.updates_buffer.push_updates()
    assert dev_context.db.get_collection("stock").count_documents({}) == 1


@pytest.fixture
def ingest_stock_t2(dev_context, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=ampel_logger)
    logd = LogsBufferDict(
        {
            "logs": [],
            "extra": {},
            "err": False,
        }
    )
    ingester = StockT2Ingester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    )
    ingester.add_ingest_models("TEST_CHANNEL", [T2IngestModel(unit="DummyStockT2Unit")])
    ingester.ingest("stockystock", [("TEST_CHANNEL", True)])
    ingester.updates_buffer.push_updates()
    assert dev_context.db.get_collection("t2").count_documents({}) == 1


@pytest.fixture(params=["DummyStateT2Unit", "DummyPointT2Unit", "DummyStockT2Unit"])
def ingest_tied_t2(dev_context, ampel_logger, request, monkeypatch):
    """Create a T2 document with dependencies"""

    tied_config_id = dev_context.add_config_id(
        {"t2_dependency": [{"unit": request.param}]}
    )
    monkeypatch.setattr(
        "ampel.test.dummy.DummyTiedStateT2Unit._unit",
        request.param,
    )

    run_id = 0
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=ampel_logger)
    logd = LogsBufferDict(
        {
            "logs": [],
            "extra": {},
            "err": False,
        }
    )
    filter_results = [("TEST_CHANNEL", True)]

    StockIngester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    ).ingest("stockystock", filter_results, {})

    # mimic a datapoint ingester
    datapoints = [
        {"_id": i, "stock": "stockystock", "body": {"thing": i + 1}} for i in range(3)
    ]
    for dp in datapoints:
        updates_buffer.add_t0_update(InsertOne(dp))

    # create compounds
    comp_ingester = DummyCompoundIngester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    )
    comp_ingester.add_channel(filter_results[0][0])
    blueprint = comp_ingester.ingest("stockystock", datapoints, filter_results)

    # create T2 doc
    # FIXME: should dependent docs be created implicitly?
    state_ingester = DummyStateT2Ingester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    )
    point_ingester = PointT2Ingester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    )
    stock_ingester = StockT2Ingester(
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=dev_context,
    )

    # NB: ingestion configured in reverse order so that t2 execution is forced
    # to explicitly resolve dependencies

    state_ingester.add_ingest_models(
        filter_results[0][0],
        [
            T2IngestModel(
                unit="DummyTiedStateT2Unit",
                config=tied_config_id,
            )
        ],
    )

    if "state" in request.param.lower():
        state_ingester.add_ingest_models(
            filter_results[0][0], [T2IngestModel(unit=request.param)]
        )
    elif "point" in request.param.lower():
        point_ingester.add_ingest_models(
            filter_results[0][0], [T2IngestModel(unit=request.param)]
        )
    elif "stock" in request.param.lower():
        stock_ingester.add_ingest_models(
            filter_results[0][0], [T2IngestModel(unit=request.param)]
        )

    state_ingester.ingest("stockystock", blueprint, filter_results)
    point_ingester.ingest("stockystock", datapoints, filter_results)
    stock_ingester.ingest("stockystock", filter_results)

    updates_buffer.push_updates()

    assert (
        dev_context.db.get_collection("t2").count_documents({"unit": request.param})
        == len(datapoints)
        if "point" in request.param.lower()
        else 1
    )
    assert (
        dev_context.db.get_collection("t2").count_documents(
            {"unit": "DummyTiedStateT2Unit"}
        )
        == 1
    )

    return request
