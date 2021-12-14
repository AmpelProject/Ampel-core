from ampel.content.T2Document import T2Document
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.enum.DocumentCode import DocumentCode
from contextlib import contextmanager

from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.t2.T2Worker import T2Worker
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


def test_metrics(dev_context, ingest_stock_t2):
    t2 = T2Worker(context=dev_context, raise_exc=True, process_name="t2")

    stats = {}
    with collect_diff(stats):
        assert t2.run() == 1

    assert stats[("ampel_t2_docs_processed_total", (("unit", "DummyStockT2Unit"),))] == 1
    assert stats[("ampel_t2_latency_seconds_sum", (("unit", "DummyStockT2Unit"),))] > 0


def test_error_reporting(dev_context: DevAmpelContext):
    dev_context.register_unit(DummyPointT2Unit)
    # DummyPointT2Unit will raise an error on the malformed T0 doc
    dev_context.db.get_collection("t0").insert_one({
        "id": 42
    })
    channels = ["channel_a", "channel_b"]
    doc: T2Document = {
        "unit": "DummyPointT2Unit",
        "code": DocumentCode.NEW,
        "config": None,
        "col": "t0",
        "stock": 42,
        "link": 42,
        "channel": channels,
        "meta": [
            {"ts": 0, "tier": 2}
        ]
    }
    dev_context.db.get_collection("t2").insert_one(doc)
    t2 = T2Worker(context=dev_context, raise_exc=False, process_name="t2", run_dependent_t2s=True)
    assert t2.run() == 1
    assert (doc := dev_context.db.get_collection("t2").find_one({}))
    assert doc["code"] == DocumentCode.EXCEPTION
    assert (trouble := dev_context.db.get_collection("troubles").find_one({}))
    assert trouble["channel"] == channels

def test_tied_t2s(dev_context, ingest_tied_t2):
    assert (num_dps := dev_context.db.get_collection("t0").count_documents({}))
    t2 = T2Worker(context=dev_context, raise_exc=True, process_name="t2", run_dependent_t2s=True)

    num_docs = t2.run()
    t2 = dev_context.db.get_collection("t2")
    assert t2.count_documents({}) == num_docs

    if "point" in ingest_tied_t2.param.lower():
        assert num_docs == 1+num_dps
        assert next(t2.find({"unit": "DummyPointT2Unit"}).sort([("link", -1)]).limit(1))["body"][0]["thing"] == 3
        assert next(t2.find({"unit": "DummyTiedStateT2Unit"}).sort([("link", -1)]).limit(1))["body"][0]["thing"] == 2*3
    elif "stock" in ingest_tied_t2.param.lower():
        assert num_docs == 2
        assert t2.find_one({"unit": "DummyStockT2Unit"})["body"][0]["id"] == "stockystock"
        assert t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["id"] == 2*"stockystock"
    else:
        assert num_docs == 2
        assert t2.find_one({"unit": "DummyStateT2Unit"})["body"][0]["len"] == num_dps
        assert t2.find_one({"unit": "DummyTiedStateT2Unit"})["body"][0]["len"] == 2*num_dps
