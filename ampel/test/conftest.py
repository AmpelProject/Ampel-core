import datetime
import json
import subprocess
from os import environ
from pathlib import Path, PosixPath
from typing import Any

import mongomock
import pymongo
import pytest

from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.content.JournalRecord import JournalRecord
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.ingest.ChainedIngestionHandler import ChainedIngestionHandler
from ampel.ingest.StockCompiler import StockCompiler
from ampel.ingest.T2Compiler import T2Compiler
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.ingest.CompilerOptions import CompilerOptions
from ampel.model.ingest.IngestBody import IngestBody
from ampel.model.ingest.IngestDirective import IngestDirective
from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T2Compute import T2Compute
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.mongo.update.MongoStockIngester import MongoStockIngester
from ampel.mongo.update.MongoT2Ingester import MongoT2Ingester
from ampel.test.dummy import (
    DummyPointT2Unit,
    DummyStateT2Unit,
    DummyStockT2Unit,
    DummyTiedStateT2Unit,
)
from ampel.util.config import get_unit_confid


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run docker-based integration tests",
    )


@pytest.fixture(scope="session")
def mongod(pytestconfig):
    if port := environ.get("MONGO_PORT"):
        yield f"mongodb://localhost:{port}"
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
        with pymongo.MongoClient(port=int(port)) as client:
            list(client.list_databases())
        yield f"mongodb://localhost:{port}"
    finally:
        ...
        subprocess.check_call(["docker", "stop", container])


@pytest.fixture
def _patch_mongo(monkeypatch):
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
def mock_context(_patch_mongo, testing_config: PosixPath):
    return DevAmpelContext.load(config=str(testing_config), purge_db=True)


@pytest.fixture
def integration_context(mongod, testing_config: PosixPath):
    ctx = DevAmpelContext.load(
        config=str(testing_config),
        purge_db=True,
        custom_conf={"resource.mongo": mongod},
    )
    yield ctx
    ctx.db.close()


# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_context", "integration_context"])
def dev_context(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def ampel_logger():
    return AmpelLogger.get_logger()


@pytest.fixture
def _ingest_stock(integration_context, ampel_logger):
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
    compiler.commit(ingester, datetime.datetime.now(tz=datetime.timezone.utc))
    ingester.updates_buffer.push_updates()
    assert integration_context.db.get_collection("stock").count_documents({}) == 1


@pytest.fixture
def _ingest_stock_t2(integration_context: DevAmpelContext, ampel_logger):
    run_id = 0
    updates_buffer = DBUpdatesBuffer(integration_context.db, run_id=run_id, logger=ampel_logger)
    stock_id = "stockystock"
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    t2_compiler = T2Compiler(run_id=run_id, tier=2)
    t2_compiler.add(
        unit="DummyStockT2Unit",
        config=None,
        stock=stock_id,
        link=stock_id,
        channel="TEST_CHANNEL",
        ttl=None,
        traceid={},
    )
    t2_compiler.commit(
        MongoT2Ingester(updates_buffer=updates_buffer), now
    )
    
    stock_compiler = StockCompiler(run_id=run_id, tier=2)
    stock_compiler.add(stock_id, "TEST_CHANNEL", {"tier": 2})
    stock_compiler.commit(
        MongoStockIngester(updates_buffer=updates_buffer), now
    )
    
    updates_buffer.push_updates()
    assert integration_context.db.get_collection("stock").count_documents({}) == 1
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
    unit_conf = {"t2_dependency": [{"unit": dependency}]}
    tied_config_id = get_unit_confid(integration_context.loader, unit="DummyTiedStateT2Unit", config=unit_conf)
    integration_context.add_conf_id(tied_config_id, unit_conf)
    run_id = 0
    channel = "TEST_CHANNEL"
    #now = datetime.datetime.now()
    updates_buffer = DBUpdatesBuffer(integration_context.db, run_id=run_id, logger=ampel_logger)

    if "stock" in dependency.lower():
        body = IngestBody(
            stock_t2=[T2Compute(unit=dependency)],
            combine=[T1Combine(unit="T1SimpleCombiner", state_t2 = [T2Compute(unit="DummyTiedStateT2Unit", config=tied_config_id)])],
        )
    elif "point" in dependency.lower():
        body = IngestBody(
            point_t2=[T2Compute(unit=dependency)],
            combine=[
                T1Combine(unit="T1SimpleCombiner", state_t2=[T2Compute(unit="DummyTiedStateT2Unit", config=tied_config_id)])
            ],
        )
    else:
        body = IngestBody(
            combine=[
                T1Combine(unit="T1SimpleCombiner", state_t2=[T2Compute(unit=dependency), T2Compute(unit="DummyTiedStateT2Unit", config=tied_config_id)])
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
