from ampel.content.JournalRecord import JournalRecord
import mongomock, pytest
import pymongo
from os import environ
from pathlib import Path, PosixPath
from typing import Any
import subprocess
import json
import datetime

from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.dev.DevAmpelContext import DevAmpelContext

from ampel.mongo.update.MongoStockIngester import MongoStockIngester
from ampel.mongo.update.MongoT2Ingester import MongoT2Ingester
from ampel.ingest.T2Compiler import T2Compiler
from ampel.ingest.StockCompiler import StockCompiler
from ampel.log.AmpelLogger import AmpelLogger

from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T2Compute import T2Compute
from ampel.model.ingest.IngestBody import IngestBody
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.CompilerOptions import CompilerOptions
from ampel.model.ingest.IngestDirective import IngestDirective

from ampel.ingest.ChainedIngestionHandler import ChainedIngestionHandler


from ampel.test.dummy import (
    DummyStockT2Unit,
    DummyPointT2Unit,
    DummyStateT2Unit,
    DummyTiedStateT2Unit,
)


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
            ["docker", "run", "--rm", "-d", "-P", "mongo:4.4"]
        ).decode().strip()
    except FileNotFoundError:
        pytest.skip("integration tests require docker")
    try:
        port = json.loads(subprocess.check_output(["docker", "inspect", container]))[0][
            "NetworkSettings"
        ]["Ports"]["27017/tcp"][0]["HostPort"]
        # wait for startup
        list(pymongo.MongoClient(port=int(port)).list_databases())
        yield f"mongodb://localhost:{port}"
    finally:
        ...
        subprocess.check_call(["docker", "stop", container])


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.core.AmpelDB.MongoClient", mongomock.MongoClient)
    # ignore codec_options in DataLoader
    monkeypatch.setattr("mongomock.codec_options.is_supported", lambda *args: None)


@pytest.fixture(scope="session")
def testing_config():
    return Path(__file__).parent / "test-data" / "testing-config.yaml"


@pytest.fixture(scope="session")
def core_config():
    """
    The config distributed with ampel-core
    """
    cb = DistConfigBuilder(options=DisplayOptions())
    for ext in "json", "yml", "yaml":
        cb.load_distrib("ampel-core", "conf", ext)
    return cb.build_config(config_validator=None, get_unit_env=False)


@pytest.fixture
def mock_context(patch_mongo, testing_config: PosixPath):
    return DevAmpelContext.load(config=str(testing_config), purge_db=True)


@pytest.fixture
def integration_context(mongod, testing_config: PosixPath):
    return DevAmpelContext.load(
        config=str(testing_config),
        purge_db=True,
        custom_conf={"resource.mongo": mongod},
    )


# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_context", "integration_context"])
def dev_context(request):
    yield request.getfixturevalue(request.param)


@pytest.fixture
def ampel_logger():
    return AmpelLogger.get_logger()


@pytest.fixture
def ingest_stock(integration_context, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(integration_context.db, run_id=run_id, logger=ampel_logger)
    ingester = MongoStockIngester(
        updates_buffer=updates_buffer,
    )
    compiler = StockCompiler(
        run_id=run_id,
        tier=0,
    )
    compiler.add(
        "stockystock",
        channel="TEST_CHANNEL",
        journal=JournalRecord(tier=0, extra={"alert": 123}),
    )
    compiler.commit(ingester, datetime.datetime.now())
    ingester.updates_buffer.push_updates()
    assert integration_context.db.get_collection("stock").count_documents({}) == 1


@pytest.fixture
def ingest_stock_t2(integration_context: DevAmpelContext, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(integration_context.db, run_id=run_id, logger=ampel_logger)
    ingester = MongoT2Ingester(
        updates_buffer=updates_buffer,
    )
    stock_id = "stockystock"
    compiler = T2Compiler(run_id=run_id, tier=2)
    compiler.add(
        unit="DummyStockT2Unit",
        config=None,
        stock=stock_id,
        link=stock_id,
        channel="TEST_CHANNEL",
        traceid={},
    )
    compiler.commit(ingester, datetime.datetime.now().timestamp())
    ingester.updates_buffer.push_updates()
    assert integration_context.db.get_collection("t2").count_documents({}) == 1

    integration_context.register_unit(DummyStockT2Unit)


@pytest.fixture(params=["DummyStateT2Unit", "DummyPointT2Unit", "DummyStockT2Unit"])
def ingest_tied_t2(integration_context: DevAmpelContext, ampel_logger, request):
    """Create a T2 document with dependencies"""

    for unit in (
        DummyStockT2Unit,
        DummyPointT2Unit,
        DummyStateT2Unit,
        DummyTiedStateT2Unit,
    ):
        integration_context.register_unit(unit)

    dependency = request.param
    tied_config_id = integration_context.gen_config_id(
        unit="DummyTiedStateT2Unit",
        arg={"t2_dependency": [{"unit": dependency}]},
        logger=ampel_logger,
    )

    run_id = 0
    channel = "TEST_CHANNEL"
    #now = datetime.datetime.now()
    updates_buffer = DBUpdatesBuffer(integration_context.db, run_id=run_id, logger=ampel_logger)

    if "stock" in dependency.lower():
        body = IngestBody(
            stock_t2=[T2Compute(unit=dependency)],
            combine=[
                T1Combine(unit="T1SimpleCombiner", state_t2=[T2Compute(unit="DummyTiedStateT2Unit", config=tied_config_id)])  # type: ignore[arg-type]
            ],
        )
    elif "point" in dependency.lower():
        body = IngestBody(
            point_t2=[T2Compute(unit=dependency)],
            combine=[
                T1Combine(unit="T1SimpleCombiner", state_t2=[T2Compute(unit="DummyTiedStateT2Unit", config=tied_config_id)])  # type: ignore[arg-type]
            ],
        )
    else:
        body = IngestBody(
            combine=[
                T1Combine(unit="T1SimpleCombiner", state_t2=[T2Compute(unit=dependency), T2Compute(unit="DummyTiedStateT2Unit", config=tied_config_id)])  # type: ignore[arg-type]
            ]
        )

    hander = ChainedIngestionHandler(
        integration_context,
        tier=0,
        run_id=run_id,
        trace_id={},
        updates_buffer=updates_buffer,
        logger=ampel_logger,
        compiler_opts=CompilerOptions(),
        shaper=UnitModel(unit="NoShaper"),
        directives=[IngestDirective(channel=channel, ingest=body)],
    )

    datapoints: list[dict[str, Any]] = [
        {"id": i, "stock": "stockystock", "body": {"thing": i + 1}} for i in range(3)
    ]

    hander.ingest(datapoints, [(0, True)], stock_id="stockystock", jm_extra={"alert": 123})

    updates_buffer.push_updates()

    assert (
        integration_context.db.get_collection("t2").count_documents({"unit": request.param})
        == len(datapoints)
        if "point" in request.param.lower()
        else 1
    )
    assert (
        integration_context.db.get_collection("t2").count_documents(
            {"unit": "DummyTiedStateT2Unit"}
        )
        == 1
    )

    return request
