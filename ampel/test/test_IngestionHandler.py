import contextlib
import datetime
from collections import defaultdict
from collections.abc import Generator
from itertools import count
from typing import Any

import pytest
from pymongo.errors import DuplicateKeyError
from pytest_mock import MockerFixture, MockFixture

from ampel.abstract.AbsIngester import AbsIngester
from ampel.config.AmpelConfig import AmpelConfig
from ampel.content.DataPoint import DataPoint
from ampel.content.MetaRecord import MetaRecord
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.ingest.ChainedIngestionHandler import ChainedIngestionHandler, IngestBody
from ampel.log.AmpelLogger import DEBUG, AmpelLogger
from ampel.model.ingest.CompilerOptions import CompilerOptions
from ampel.model.ingest.IngestDirective import IngestDirective
from ampel.model.ingest.MuxModel import MuxModel
from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T2Compute import T2Compute
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.MongoIngester import MongoIngester
from ampel.queue.QueueIngester import AbsProducer, QueueIngester
from ampel.test.dummy import (
    DummyHistoryMuxer,
    DummyMuxer,
    DummyPointT2Unit,
    DummyStateT2Unit,
    DummyStockT2Unit,
)
from ampel.util.freeze import recursive_unfreeze


@pytest.fixture
def _dummy_units(dev_context: DevAmpelContext):
    for unit in (
        DummyStockT2Unit,
        DummyPointT2Unit,
        DummyStateT2Unit,
        DummyMuxer,
        DummyHistoryMuxer,
    ):
        dev_context.register_unit(unit)


@pytest.fixture(params=["T1SimpleCombiner", "T1SimpleRetroCombiner"])
def single_source_directive(
    dev_context: DevAmpelContext, _dummy_units, request
) -> IngestDirective:
    return IngestDirective(
        channel="TEST_CHANNEL",
        ingest=IngestBody(
            # https://github.com/python/mypy/issues/13421
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
    dev_context: DevAmpelContext, _dummy_units, request
) -> IngestDirective:
    return IngestDirective(
        channel="TEST_CHANNEL",
        ingest=IngestBody(
            stock_t2=[T2Compute(unit="DummyStockT2Unit")],
            mux=MuxModel(
                unit="DummyMuxer",
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


def get_handler(
    context: DevAmpelContext,
    directives,
    ingester_model=UnitModel(unit="MongoIngester"),  # noqa: B008
) -> ChainedIngestionHandler:
    run_id = 0
    logger = AmpelLogger.get_logger(console={"level": DEBUG})
    ingester = context.loader.new_context_unit(
        ingester_model,
        context=context,
        sub_type=AbsIngester,
        logger=logger,
        run_id=run_id,
        tier=0,
        process_name="ingest",
        raise_exc=True,
        # need to disable provenance for dynamically registered unit
        _provenance=False,
    )
    return ChainedIngestionHandler(
        context=context,
        logger=logger,
        shaper=UnitModel(unit="NoShaper"),
        tier=0,
        trace_id={},
        run_id=run_id,
        compiler_opts=CompilerOptions(t0={"tag": ["TAGGERT"]}),
        ingester=ingester,
        directives=directives,
    )


def test_no_directive(dev_context):
    with pytest.raises(ValueError, match="Need at least 1 directive"):
        get_handler(dev_context, [])


@pytest.fixture
def datapoints() -> list[DataPoint]:
    return [
        {"id": i, "stock": "stockystock", "body": {"thing": i}}  # type: ignore[typeddict-item]
        for i in range(3)
    ]


def test_minimal_directive(
    dev_context: DevAmpelContext, datapoints: list[dict[str, Any]]
):
    """
    Minimal directive creates stock + t0 docs
    """
    directive = IngestDirective(channel="TEST_CHANNEL")
    handler = get_handler(dev_context, [directive])
    assert isinstance(handler.ingester, MongoIngester)

    # ingestion is idempotent
    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.ingester._updates_buffer.push_updates()
    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.ingester._updates_buffer.push_updates()

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
    assert isinstance(handler.ingester, MongoIngester)

    # include retro-completed states if configured
    assert single_source_directive.ingest.combine
    retro = "retro" in str(single_source_directive.ingest.combine[0].unit).lower()
    num_points = len(datapoints)
    num_states = num_points if retro else 1

    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.ingester._updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")
    t2 = dev_context.db.get_collection("t2")

    assert t0.count_documents({}) == len(datapoints)
    assert t0.count_documents({"stock": "stockystock"}) == len(datapoints)
    assert t0.count_documents({"id": 0}) == 1

    assert t1.count_documents({}) == num_states if retro else 1

    check_unit_counts(list(t2.find({})), num_states, num_points)

    # test MongoT2Ingester idempotency
    before = t2.find_one_and_update({}, {"$set": {"code": DocumentCode.OK}})
    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.ingester._updates_buffer.push_updates()
    after = t2.find_one({"_id": before["_id"]})
    assert before["code"] == DocumentCode.NEW
    assert after["code"] == DocumentCode.OK, "code was not overwitten"
    assert before["body"] == after["body"]
    assert len(after["meta"]) > len(before["meta"])


def test_queue_ingester(
    dev_context,
    single_source_directive: IngestDirective,
    datapoints,
    mocker: MockerFixture,
):
    @dev_context.register_unit
    class DummyProducer(AbsProducer):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self.items: list[AbsProducer.Item] = []

        def produce(self, item: AbsProducer.Item, delivery_callback=None):
            self.items.append(item)
            if delivery_callback:
                delivery_callback()

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            return

    acknowledge_callback = mocker.MagicMock()

    dev_context.register_unit(QueueIngester)
    handler = get_handler(
        dev_context,
        [single_source_directive],
        UnitModel(
            unit="QueueIngester",
            config={
                "producer": {
                    "unit": "DummyProducer",
                },
                "acknowledge_callback": acknowledge_callback,
            },
        ),
    )
    assert isinstance(handler.ingester, QueueIngester)
    assert isinstance(handler.ingester._producer, DummyProducer)

    # include retro-completed states if configured
    assert single_source_directive.ingest.combine
    retro = "retro" in str(single_source_directive.ingest.combine[0].unit).lower()
    num_points = len(datapoints)
    num_states = num_points if retro else 1

    sentinel = object()

    with handler.ingester.group([sentinel]):
        handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    assert len(items := handler.ingester._producer.items) == 1
    assert acknowledge_callback.call_count == 1, "acknowledge callback called"
    assert list(acknowledge_callback.call_args[0][0]) == [sentinel], "callback payload contains sentinel"
    with handler.ingester.group():
        handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    assert len(items := handler.ingester._producer.items) == 2, (
        "second ingestion creates a second message"
    )
    assert acknowledge_callback.call_count == 1, "acknowledge callback called only once"

    t0 = items[0].t0
    t1 = items[0].t1
    t2 = items[0].t2

    assert len(t0) == len(datapoints)
    assert all(doc["stock"] == "stockystock" for doc in t0)
    assert len([d for d in t0 if d["id"] == 0]) == 1

    assert len(t1) == num_states if retro else 1

    check_unit_counts(t2, num_states, num_points)


def test_multiplex_directive(
    dev_context, multiplex_directive: IngestDirective, datapoints
):
    handler = get_handler(dev_context, [multiplex_directive])
    assert isinstance(handler.ingester, MongoIngester)

    # include retro-completed states if configured
    assert multiplex_directive.ingest.mux
    assert multiplex_directive.ingest.mux.combine
    retro = "retro" in str(multiplex_directive.ingest.mux.combine[0].unit).lower()
    num_points = len(datapoints) + 5
    num_states = num_points if retro else 1

    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.ingester._updates_buffer.push_updates()

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
    assert isinstance(handler.ingester, MongoIngester)

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
    handler.ingester._updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")

    assert t0.count_documents({"channel": "TEST_CHANNEL"}) == len(datapoints), (
        "single-stream channel contains only direct datapoints"
    )
    assert t0.count_documents({"channel": "LONG_CHANNEL"}) == num_points, (
        "multiplexed channel contains additional datapoints"
    )

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
    assert isinstance(handler.ingester, MongoIngester)

    handler.ingest(datapoints, [(0, True)], stock_id="stockystock")
    handler.ingester._updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")

    assert t0.count_documents({"channel": "TEST_CHANNEL"}) == len(datapoints), (
        "single-stream channel contains only direct datapoints"
    )
    assert t0.count_documents({"channel": "LONG_CHANNEL"}) == 0, (
        "multiplexed channel contains additional datapoints"
    )


def test_t0_meta_append(
    mock_context: DevAmpelContext,
    datapoints: list[DataPoint],
):
    """T0Compiler preserves tags and meta entries"""
    handler = get_handler(mock_context, [IngestDirective(channel="TEST_CHANNEL")])
    assert isinstance(handler.ingester, MongoIngester)
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
    handler.t0_compiler.add(datapoints, "SOME_CHANNEL", None, trace_id=0)
    handler.t0_compiler.commit(handler.ingester.t0, ts)
    handler.ingester._updates_buffer.push_updates(force=True)

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
    writable_config = recursive_unfreeze(config.get())  # type: ignore[arg-type]
    context.config = AmpelConfig(writable_config, freeze=False)
    yield writable_config
    context.config = config


def test_duplicate_t0_id(
    integration_context: DevAmpelContext,
    datapoints: list[DataPoint],
):
    def run() -> None:
        handler = get_handler(
            integration_context, [IngestDirective(channel="TEST_CHANNEL")]
        )
        assert isinstance(handler.ingester, MongoIngester)
        handler.t0_compiler.add(datapoints, "SOME_CHANNEL", ttl=None, trace_id=0)
        handler.t0_compiler.commit(handler.ingester.t0, 0)
        handler.ingester._updates_buffer.push_updates(force=True)

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


def test_t0_ttl(
    mock_context: DevAmpelContext,
    datapoints: list[DataPoint],
    mocker: MockFixture,
):
    """
    Previously inserted datapoint ttls are updated when used by a muxer
    """
    mocker.patch(
        "ampel.ingest.ChainedIngestionHandler.time", side_effect=count().__next__
    )
    mock_context.register_units(DummyHistoryMuxer)

    ttl = datetime.timedelta(seconds=300)
    with unfreeze_config(mock_context) as config:
        config["mongo"]["ingest"]["t0"] = {
            "unit": "MongoT0Ingester",
            "config": {"update_ttl": True},
        }
        config["channel"]["TEST_CHANNEL"]["purge"]["content"]["delay"] = {
            "seconds": ttl.total_seconds()
        }

        handler = get_handler(
            mock_context,
            [
                IngestDirective(
                    channel="TEST_CHANNEL",
                    ingest={  # type: ignore[arg-type]
                        "mux": {
                            "unit": "DummyHistoryMuxer",
                            "combine": [
                                {
                                    "unit": "T1SimpleCombiner",
                                    "state_t2": [{"unit": "DummyStateT2Unit"}],
                                }
                            ],
                        }
                    },
                )
            ],
        )

    def ingest(datapoints: list[DataPoint]):
        handler.ingest(
            [dict(dp) for dp in datapoints], [(0, True)], stock_id="stockystock"
        )
        assert isinstance(handler.ingester, MongoIngester)
        handler.ingester._updates_buffer.push_updates(force=True)

    def get_meta_time(dp: DataPoint | T1Document | T2Document) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(
            dp["meta"][-1]["ts"], tz=datetime.timezone.utc
        )

    def get_expire_time(dp: DataPoint | T1Document | T2Document):
        # NB: mongo datetimes are implicitly utc, so reattach tzinfo
        return dp["expiry"].replace(tzinfo=datetime.timezone.utc)

    ingest(datapoints[:1])
    for tier in range(3):
        assert (
            collection := mock_context.db.get_collection(f"t{tier}")
        ).count_documents({}) == 1, f"t{tier} document inserted"
        doc: None | DataPoint | T1Document | T2Document = collection.find_one()
        assert doc is not None
        assert get_expire_time(doc) == get_meta_time(doc) + ttl, (
            f"ttl set on t{tier} document"
        )

    # ingest remaining datapoints
    ingest(datapoints)
    dp: None | DataPoint = mock_context.db.get_collection("t0").find_one({"id": 0})
    assert dp is not None
    assert get_expire_time(dp) > get_meta_time(dp) + ttl, (
        f"ttl updated for dp {dp['id']}"
    )
    assert len(dp["meta"]) == 1, "no extra meta entries added"

    # and again, ensuring that all ttls get updated
    ingest(datapoints)
    for dp in mock_context.db.get_collection("t0").find():
        assert dp is not None
        assert get_expire_time(dp) > get_meta_time(dp) + ttl, (
            f"ttl updated for dp {dp['id']}"
        )
        assert len(dp["meta"]) == 1, "no extra meta entries added"
