import inspect
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
            if "prometheus_multiproc_dir" in os.environ:
                MultiProcessCollector(cls._registry)
        return cls._registry

    @classmethod
    def collect(cls):
        yield from cls.registry().collect()

    @staticmethod
    def _get_name(level: int = 2) -> str:
        parts = inspect.getmodule(inspect.stack()[level][0]).__name__.split(".")
        return "_".join(parts if parts[0] != "ampel" else parts[1:])

    @classmethod
    def counter(
        cls, name, documentation, unit="", labelnames=(), subsystem=None
    ) -> Counter:
        return Counter(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem or cls._get_name(2),
            registry=cls.registry(),
        )

    @classmethod
    def gauge(
        cls, name, documentation, unit="", labelnames=(), subsystem=None
    ) -> Gauge:
        return Gauge(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem or cls._get_name(2),
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
        subsystem=None,
        buckets=Histogram.DEFAULT_BUCKETS,
    ) -> Histogram:
        return Histogram(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem or cls._get_name(2),
            registry=cls.registry(),
            buckets=buckets,
        )
