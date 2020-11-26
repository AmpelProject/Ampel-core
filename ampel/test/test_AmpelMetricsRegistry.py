from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry


def test_autoname():
    assert (
        AmpelMetricsRegistry.counter("foo", "foos")._name
        == "ampel_test_AmpelMetricsRegistry_foo"
    )
