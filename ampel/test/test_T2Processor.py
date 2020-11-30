from contextlib import contextmanager

from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.t2.T2Processor import T2Processor


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
    t2 = T2Processor(context=dev_context, raise_exc=True, process_name="t2")

    stats = {}
    with collect_diff(stats):
        assert t2.run() == 1

    assert stats[("ampel_t2_docs_processed_total", (("unit", "CaptainObvious"),))] == 1
    assert stats[("ampel_t2_latency_seconds_sum", (("unit", "CaptainObvious"),))] > 0
