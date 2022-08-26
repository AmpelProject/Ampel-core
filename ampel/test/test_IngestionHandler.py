import contextlib, pytest
from typing import Any
from collections import defaultdict
from collections.abc import Generator
from ampel.config.AmpelConfig import AmpelConfig
from ampel.content.MetaRecord import MetaRecord
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.util.freeze import recursive_unfreeze
from pymongo.errors import DuplicateKeyError

from ampel.content.DataPoint import DataPoint
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.ingest.ChainedIngestionHandler import ChainedIngestionHandler, IngestBody
from ampel.log.AmpelLogger import DEBUG, AmpelLogger
from ampel.model.ingest.CompilerOptions import CompilerOptions
from ampel.model.ingest.IngestDirective import IngestDirective
from ampel.model.ingest.MuxModel import MuxModel
from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T2Compute import T2Compute
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.mongo.update.MongoStockIngester import MongoStockIngester
from ampel.mongo.update.MongoT0Ingester import MongoT0Ingester
from ampel.mongo.update.MongoT1Ingester import MongoT1Ingester
from ampel.mongo.update.MongoT2Ingester import MongoT2Ingester
from ampel.test.dummy import (
    DummyMuxer,
    DummyPointT2Unit,
    DummyStateT2Unit,
    DummyStockT2Unit,
)


@pytest.fixture
def dummy_units(dev_context: DevAmpelContext):
    for unit in (DummyStockT2Unit, DummyPointT2Unit, DummyStateT2Unit, DummyMuxer):
        dev_context.register_unit(unit) # type: ignore


@pytest.fixture(params=["T1SimpleCombiner", "T1SimpleRetroCombiner"])
def single_source_directive(
    dev_context: DevAmpelContext, dummy_units, request
) -> IngestDirective:

    return IngestDirective(
        channel="TEST_CHANNEL",
        ingest=IngestBody(
            # https://github.com/python/mypy/issues/13421
            stock_t2=[T2Compute(unit="DummyStockT2Unit")], # type: ignore[arg-type]
            point_t2=[T2Compute(unit="DummyPointT2Unit")], # type: ignore[arg-type]
            combine=[
                T1Combine(
                    unit=request.param,
                    state_t2=[T2Compute(unit="DummyStateT2Unit")], # type: ignore[arg-type]
                )
            ],
        ),
    )


@pytest.fixture(params=["T1SimpleCombiner", "T1SimpleRetroCombiner"])
def multiplex_directive(
    dev_context: DevAmpelContext, dummy_units, request
) -> IngestDirective:

    return IngestDirective(
        channel="TEST_CHANNEL",
        ingest=IngestBody(
            stock_t2=[T2Compute(unit="DummyStockT2Unit")], # type: ignore[arg-type]
            mux=MuxModel(
                unit="DummyMuxer",  # type: ignore[arg-type]
                insert={"point_t2": [T2Compute(unit="DummyPointT2Unit")]}, # type: ignore[arg-type]
                combine=[
                    T1Combine(
                        unit=request.param,
                        state_t2=[T2Compute(unit="DummyStateT2Unit")], # type: ignore[arg-type]
                    )
                ],
            ),
        ),
    )


def get_handler(context: DevAmpelContext, directives) -> ChainedIngestionHandler:
    run_id = 0
    logger = AmpelLogger.get_logger(console={"level": DEBUG})
    updates_buffer = DBUpdatesBuffer(
        context.db, run_id=run_id, logger=logger, raise_exc=True
    )
    return ChainedIngestionHandler(
        context=context,
        logger=logger,
        shaper=UnitModel(unit="NoShaper"),
        tier=0,
        trace_id={},
        run_id=0,
        compiler_opts=CompilerOptions(t0={"tag": ["TAGGERT"]}),
        updates_buffer=updates_buffer,
        directives=directives,
    )


def test_no_directive(dev_context):
    with pytest.raises(ValueError):
        get_handler(dev_context, [])


@pytest.fixture
def datapoints() -> list[DataPoint]:
    return [
        {"id": i, "stock": "stockystock", "body": {"thing": i}} # type: ignore[typeddict-item]
        for i in range(3)
    ]


def test_minimal_directive(dev_context: DevAmpelContext, datapoints: list[dict[str, Any]]):
    """
    Minimal directive creates stock + t0 docs
    """
    directive = IngestDirective(channel="TEST_CHANNEL")
    handler = get_handler(dev_context, [directive])
    assert isinstance(handler.stock_ingester, MongoStockIngester)
    assert isinstance(handler.t0_ingester, MongoT0Ingester)
    assert isinstance(handler.t1_ingester, MongoT1Ingester)
    assert isinstance(handler.t2_ingester, MongoT2Ingester)

    # ingestion is idempotent
    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.updates_buffer.push_updates()
    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.updates_buffer.push_updates()

    stock = dev_context.db.get_collection("stock")
    assert stock.count_documents({}) == 1
    assert stock.count_documents({"stock": "stockystock"}) == 1

    for tier in range(4):
        col = dev_context.db.get_collection(f"t{tier}")
        assert col.count_documents({}) == (len(datapoints) if tier == 0 else 0)


def check_unit_counts(docs, num_states, num_points):
    assert len(docs) == 1 + num_states + num_points
    by_unit = defaultdict(list)
    for doc in docs:
        by_unit[doc["unit"]].append(doc)
    assert len(by_unit["DummyStateT2Unit"]) == num_states
    assert len(by_unit["DummyStockT2Unit"]) == 1
    assert len(by_unit["DummyPointT2Unit"]) == num_points
    for doc in docs:
        assert doc["stock"] == "stockystock"
        if doc["unit"] == "DummyStateT2Unit":
            assert isinstance(doc["link"], int)
        elif doc["unit"] == "DummyStockT2Unit":
            assert doc["link"] == "stockystock"
            assert doc["col"] == "stock"
        elif doc["unit"] == "DummyPointT2Unit":
            assert isinstance(doc["link"], int)
            assert doc["col"] == "t0"


def test_single_source_directive(
    dev_context, single_source_directive: IngestDirective, datapoints
):
    handler = get_handler(dev_context, [single_source_directive])
    assert isinstance(handler.stock_ingester, MongoStockIngester)
    assert isinstance(handler.t0_ingester, MongoT0Ingester)
    assert isinstance(handler.t1_ingester, MongoT1Ingester)
    assert isinstance(handler.t2_ingester, MongoT2Ingester)

    # include retro-completed states if configured
    assert single_source_directive.ingest.combine
    retro = "retro" in str(single_source_directive.ingest.combine[0].unit).lower()
    num_points = len(datapoints)
    num_states = num_points if retro else 1

    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")
    t2 = dev_context.db.get_collection("t2")

    assert t0.count_documents({}) == len(datapoints)
    assert t0.count_documents({"stock": "stockystock"}) == len(datapoints)
    assert t0.count_documents({"id": 0}) == 1

    assert t1.count_documents({}) == num_states if retro else 1

    check_unit_counts(list(t2.find({})), num_states, num_points)


def test_multiplex_directive(
    dev_context, multiplex_directive: IngestDirective, datapoints
):
    handler = get_handler(dev_context, [multiplex_directive])

    # include retro-completed states if configured
    assert multiplex_directive.ingest.mux
    assert multiplex_directive.ingest.mux.combine
    retro = "retro" in str(multiplex_directive.ingest.mux.combine[0].unit).lower()
    num_points = len(datapoints) + 5
    num_states = num_points if retro else 1

    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")
    t2 = dev_context.db.get_collection("t2")

    assert t0.count_documents({}) == num_points
    assert t0.count_documents({"stock": "stockystock"}) == num_points
    assert t0.count_documents({"id": 0}) == 1

    assert t1.count_documents({}) == num_states if retro else 1

    check_unit_counts(list(t2.find({})), num_states, num_points)


def test_multiplex_dispatch(
    dev_context,
    single_source_directive: IngestDirective,
    multiplex_directive: IngestDirective,
    datapoints,
):
    """
    Extended history compounds are created only for channels that request it
    """
    single_source_directive.channel = "TEST_CHANNEL"
    multiplex_directive.channel = "LONG_CHANNEL"

    handler = get_handler(dev_context, [single_source_directive, multiplex_directive])

    # include retro-completed states if configured
    assert single_source_directive.ingest.combine
    assert single_source_directive.ingest.combine[0]
    assert multiplex_directive.ingest.mux
    assert multiplex_directive.ingest.mux.combine
    assert multiplex_directive.ingest.mux.combine[0]
    retro_short = "retro" in str(single_source_directive.ingest.combine[0].unit).lower()
    retro_long = "retro" in str(multiplex_directive.ingest.mux.combine[0].unit).lower()
    num_points = len(datapoints) + 5

    handler.ingest(datapoints, [(0, True), (1, True)], stock_id="stockystock")
    handler.updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")

    assert t0.count_documents({"channel": "TEST_CHANNEL"}) == len(
        datapoints
    ), "single-stream channel contains only direct datapoints"
    assert (
        t0.count_documents({"channel": "LONG_CHANNEL"}) == num_points
    ), "multiplexed channel contains additional datapoints"

    assert t1.count_documents({"channel": "TEST_CHANNEL"}) == (
        len(datapoints) if retro_short else 1
    ), "single-stream channel has the correct number of states"
    assert t1.count_documents({"channel": "LONG_CHANNEL"}) == (
        num_points if retro_long else 1
    ), "single-stream channel has the correct number of states"
    # if retro-completion settings are different, channels' states are disjoint
    assert (
        t1.count_documents(
            {"$and": [{"channel": "TEST_CHANNEL"}, {"channel": "LONG_CHANNEL"}]}
        )
        == 0
    ), "channel states are disjoint"


def test_multiplex_elision(
    dev_context,
    single_source_directive: IngestDirective,
    multiplex_directive: IngestDirective,
    datapoints,
):
    """
    Extended history points are skipped when only short channels pass
    """
    single_source_directive.channel = "TEST_CHANNEL"
    multiplex_directive.channel = "LONG_CHANNEL"

    handler = get_handler(dev_context, [single_source_directive, multiplex_directive])

    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")

    assert t0.count_documents({"channel": "TEST_CHANNEL"}) == len(
        datapoints
    ), "single-stream channel contains only direct datapoints"
    assert (
        t0.count_documents({"channel": "LONG_CHANNEL"}) == 0
    ), "multiplexed channel contains additional datapoints"


def test_t0_meta_append(
    mock_context: DevAmpelContext,
    datapoints: list[DataPoint],
):
    """T0Compiler preserves tags and meta entries"""
    handler = get_handler(mock_context, [IngestDirective(channel="TEST_CHANNEL")])
    ts = 3.14159
    meta_record: MetaRecord = {
        "activity": [
            {
                "code": MetaActionCode.ADD_INGEST_TAG,
            }
        ],
        "ts": ts,
        "extra": {"thing": "floop"},
    }
    tags = ["SOME_TAG", "SOME_OTHER_TAG"]

    datapoints[0]["meta"] = [meta_record]
    datapoints[0]["tag"] = tags
    handler.t0_compiler.add(datapoints, "SOME_CHANNEL", trace_id=0)
    handler.t0_compiler.commit(handler.t0_ingester, ts)
    handler.updates_buffer.push_updates(force=True)

    doc = mock_context.db.get_collection("t0").find_one({"id": datapoints[0]["id"]})
    assert doc is not None
    assert doc["channel"] == ["SOME_CHANNEL"]
    assert set(tags).intersection(doc["tag"]), "initial datapoint tags set"
    assert set(doc["tag"]).difference(tags), "compiler adds its own tags"
    assert len(doc["meta"]) > 1, "initial datapoint meta set"
    assert meta_record in doc["meta"], "compiler adds its own meta entries"


@contextlib.contextmanager
def unfreeze_config(context: DevAmpelContext) -> Generator[dict, None, None]:
    config = context.config
    writable_config = recursive_unfreeze(config.get()) # type: ignore[arg-type]
    context.config = AmpelConfig(writable_config, freeze=False)
    yield writable_config
    context.config = config


def test_duplicate_t0_id(
    integration_context: DevAmpelContext,
    datapoints: list[DataPoint],
):
    def run():
        handler = get_handler(integration_context, [IngestDirective(channel="TEST_CHANNEL")])
        handler.t0_compiler.add(datapoints, "SOME_CHANNEL", trace_id=0)
        handler.t0_compiler.commit(handler.t0_ingester, 0)
        handler.updates_buffer.push_updates(force=True)

    # populate documents
    run()

    # fake an id collision by changing the body of an existing document without
    # updating its id. when the consumer runs again below, this should cause the
    # update operation to fail to match the body, and attempt to insert a duplicate
    # id
    datapoints[0]["body"]["broken"] = "boom"
    with unfreeze_config(integration_context) as config:
        config["mongo"]["ingest"]["t0"] = {
            "unit": "MongoT0Ingester",
            "config": {"extended_match": 2},
        }
        with pytest.raises(DuplicateKeyError):
            run()

    # with id check disabled, no exception is raised
    run()
