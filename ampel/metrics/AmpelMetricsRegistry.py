import os
from typing import Optional

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client.multiprocess import MultiProcessCollector


class AmpelMetricsRegistry:

    _registry: Optional[CollectorRegistry] = None

    @classmethod
    def registry(cls) -> CollectorRegistry:
        if cls._registry is None:
            cls._registry = CollectorRegistry()
        return cls._registry

    @classmethod
    def collect(cls):
        if "prometheus_multiproc_dir" in os.environ:
            yield from MultiProcessCollector(None).collect()
        else:
            yield from cls.registry().collect()

    @classmethod
    def counter(
        cls, name, documentation, unit="", labelnames=(), subsystem=""
    ) -> Counter:
        return Counter(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem,
            registry=cls.registry(),
        )

    @classmethod
    def gauge(
        cls, name, documentation, unit="", labelnames=(), subsystem=""
    ) -> Gauge:
        return Gauge(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem,
            registry=cls.registry(),
            multiprocess_mode="livesum",
        )

    @classmethod
    def histogram(
        cls,
        name,
        documentation,
        unit="",
        labelnames=(),
        subsystem="",
        buckets=Histogram.DEFAULT_BUCKETS,
    ) -> Histogram:
        return Histogram(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem,
            registry=cls.registry(),
            buckets=buckets,
        )
