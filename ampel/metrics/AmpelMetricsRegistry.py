import os
from typing import Any, ClassVar

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)
from prometheus_client.multiprocess import MultiProcessCollector
from prometheus_client.metrics import MetricWrapperBase


def reset_metric(metric: MetricWrapperBase) -> None:
    if metric._is_parent():
        for child in metric._metrics.values():
            reset_metric(child)
    elif hasattr(metric, "_value"):
        metric._value.set(0)


class AmpelMetricsRegistry(CollectorRegistry):

    _registry: ClassVar[None | CollectorRegistry] = None
    _standalone_collectors: ClassVar[list[Any]] = []

    @classmethod
    def registry(cls) -> CollectorRegistry:
        if cls._registry is None:
            cls._registry = CollectorRegistry()
        return cls._registry

    @classmethod
    def push(
        cls, gateway: str, job: str, timeout: float | None = 30, reset: bool = False
    ):
        registry = cls.registry()
        push_to_gateway(gateway, job, registry, timeout=timeout)
        if reset:
            for metric in registry._names_to_collectors.values():
                if isinstance(metric, MetricWrapperBase):
                    reset_metric(metric)

    @classmethod
    def collect(cls):
        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            yield from MultiProcessCollector(None).collect()
        else:
            yield from cls.registry().collect()
        for collector in cls._standalone_collectors:
            yield from collector.collect()

    @classmethod
    def register_collector(cls, collector):
        cls._standalone_collectors.append(collector)

    @classmethod
    def deregister_collector(cls, collector):
        cls._standalone_collectors.remove(collector)

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
        cls,
        name,
        documentation,
        unit="",
        labelnames=(),
        subsystem="",
        multiprocess_mode="livesum",
    ) -> Gauge:
        return Gauge(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem,
            registry=cls.registry(),
            multiprocess_mode=multiprocess_mode,
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
