from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry


def test_autoname():
    assert (
        AmpelMetricsRegistry.counter("foo", "foos", unit="blarghs", subsystem="blah")._name
        == "ampel_blah_foo_blarghs"
    )
