import pytest
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry, reset_registry
from prometheus_client.metrics import MetricWrapperBase


@pytest.fixture
def empty_registry():
    prev = AmpelMetricsRegistry._registry
    AmpelMetricsRegistry._registry = None
    yield AmpelMetricsRegistry.registry()
    AmpelMetricsRegistry._registry = prev


def test_autoname(empty_registry):
    assert (
        AmpelMetricsRegistry.counter(
            "foo", "foos", unit="blarghs", subsystem="blah"
        )._name
        == "ampel_blah_foo_blarghs"
    )


def verify(metric: MetricWrapperBase, value: int):
    for sample in next(iter(metric.collect())).samples:
        if not sample.name.endswith("_created"):
            assert sample.value == value


def test_reset_counter(empty_registry):
    metric = AmpelMetricsRegistry.counter("foo", "foos")
    verify(metric, 0)
    metric.inc(1)
    verify(metric, 1)
    reset_registry(empty_registry)
    verify(metric, 0)


def test_reset_summary(empty_registry):
    metric = AmpelMetricsRegistry.summary("foo", "foos")
    verify(metric, 0)
    metric.observe(1)
    verify(metric, 1)
    reset_registry(empty_registry)
    verify(metric, 0)


def test_reset_histogram(empty_registry):
    metric = AmpelMetricsRegistry.histogram("foo", "foos", buckets=[1, float("inf")])
    verify(metric, 0)
    metric.observe(1)
    verify(metric, 1)
    reset_registry(empty_registry)
    verify(metric, 0)


def test_reset_gauge(empty_registry):
    metric = AmpelMetricsRegistry.gauge("foo", "foos")
    verify(metric, 0)
    metric.set(1)
    verify(metric, 1)
    reset_registry(empty_registry)
    verify(metric, 0)
