import pytest

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
        dev_context.register_unit(unit)


@pytest.fixture(params=["T1SimpleCombiner", "T1SimpleRetroCombiner"])
def single_source_directive(
    dev_context: DevAmpelContext, dummy_units, request
) -> IngestDirective:

    return IngestDirective(
        channel="TEST_CHANNEL",
        ingest=IngestBody(
            stock_t2=[T2Compute(unit="DummyStockT2Unit")],
            point_t2=[T2Compute(unit="DummyPointT2Unit")],
            combine=[
                T1Combine(
                    unit=request.param,
                    state_t2=[T2Compute(unit="DummyStateT2Unit")],
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
            stock_t2=[T2Compute(unit="DummyStockT2Unit")],
            mux=MuxModel(
                unit="DummyMuxer", # type: ignore[arg-type]
                insert={"point_t2": [T2Compute(unit="DummyPointT2Unit")]},
                combine=[
                    T1Combine(
                        unit=request.param,
                        state_t2=[T2Compute(unit="DummyStateT2Unit")],
                    )
                ],
            ),
        ),
    )


def get_handler(context: DevAmpelContext, directives) -> ChainedIngestionHandler:
    run_id = 0
    logger = AmpelLogger.get_logger(console={"level": DEBUG})
    updates_buffer = DBUpdatesBuffer(context.db, run_id=run_id, logger=logger)
    return ChainedIngestionHandler(
        context=context,
        logger=logger,
        shaper=UnitModel(unit="NoShaper"),
        tier=0,
        trace_id={},
        run_id=0,
        compiler_opts=CompilerOptions(),
        updates_buffer=updates_buffer,
        directives=directives,
    )


def test_no_directive(dev_context):
    with pytest.raises(ValueError):
        get_handler(dev_context, [])


@pytest.fixture
def datapoints() -> list[DataPoint]:
    return [{"id": i, "stock": "stockystock"} for i in range(3)]


def test_minimal_directive(dev_context: DevAmpelContext, datapoints: list[DataPoint]):
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

    assert len(docs := list(t2.find({}))) == 1 + num_states + num_points
    for i in range(0, num_states):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyStateT2Unit"
        assert isinstance(docs[i]["link"], int)
    for i in range(num_states, num_states + 1):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyStockT2Unit"
        assert docs[i]["link"] == "stockystock"
        assert docs[i]["col"] == "stock"
    for i in range(num_states + 1, num_states + 1 + num_points):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyPointT2Unit"
        assert isinstance(docs[i]["link"], int)
        assert docs[i]["col"] == "t0"


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

    assert len(docs := list(t2.find({}))) == 1 + num_states + num_points
    for i in range(0, num_states):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyStateT2Unit"
        assert isinstance(docs[i]["link"], int)
    for i in range(num_states, num_states + 1):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyStockT2Unit"
        assert docs[i]["link"] == "stockystock"
        assert docs[i]["col"] == "stock"
    for i in range(num_states + 1, num_states + 1 + num_points):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyPointT2Unit"
        assert isinstance(docs[i]["link"], int)
        assert docs[i]["col"] == "t0"


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
    t2 = dev_context.db.get_collection("t2")

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
    t1 = dev_context.db.get_collection("t1")
    t2 = dev_context.db.get_collection("t2")

    assert t0.count_documents({"channel": "TEST_CHANNEL"}) == len(
        datapoints
    ), "single-stream channel contains only direct datapoints"
    assert (
        t0.count_documents({"channel": "LONG_CHANNEL"}) == 0
    ), "multiplexed channel contains additional datapoints"
