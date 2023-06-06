import os
from typing import Any, ClassVar, Iterable, Optional, Sequence

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    push_to_gateway,
)
from prometheus_client.multiprocess import MultiProcessCollector
from prometheus_client.metrics import MetricWrapperBase
from prometheus_client.context_managers import Timer
from time import perf_counter_ns

from prometheus_client.registry import REGISTRY, CollectorRegistry


def reset_metric(metric: MetricWrapperBase) -> None:
    if metric._is_parent():
        for child in metric._metrics.values():
            reset_metric(child)
    else:
        if isinstance(metric, (Counter, Gauge)):
            metric._value.set(0)
        elif isinstance(metric, Summary):
            metric._sum.set(0)
            metric._count.set(0)
        elif isinstance(metric, Histogram):
            metric._sum.set(0)
            for bucket in metric._buckets:
                bucket.set(0)
        else:
            raise TypeError(f"don't know how to reset metric of type {type(metric)}")


def reset_registry(registry: CollectorRegistry) -> None:
    for metric in registry._names_to_collectors.values():
        if isinstance(metric, MetricWrapperBase):
            reset_metric(metric)


class UnitTimer(Timer):
    """
    Our own version of timer that supports a custom time function (mainly for units)
    """

    def __init__(self, metric, callback_name, unit: int = 1000):
        super().__init__(metric, callback_name)
        self._unit = unit

    def timer(self) -> int:
        return perf_counter_ns() // self._unit

    def _new_timer(self):
        return self.__class__(self._metric, self._callback_name, self._unit)

    def __enter__(self):
        self._start = self.timer()
        return self

    def __exit__(self, typ, value, traceback):
        # Time can go backwards.
        duration = max(self.timer() - self._start, 0)
        callback = getattr(self._metric, self._callback_name)
        callback(duration)


class UnitTimable(MetricWrapperBase):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ns_per_time_unit = None
        if self._unit in ("seconds", "s"):
            self._ns_per_time_unit = 1_000_000_000
        elif self._unit in ("milliseconds", "millis", "ms"):
            self._ns_per_time_unit = 1_000_000
        elif self._unit in ("microseconds", "micros", "µs"):
            self._ns_per_time_unit = 1_000
        elif self._unit in ("nanoseconds", "nanos", "ns"):
            self._ns_per_time_unit = 1


# A counter that knows how to time
class TimingCounter(Counter, UnitTimable):
    def time(self) -> UnitTimer:
        if self._ns_per_time_unit is None:
            raise ValueError(f"Can't time in units of {self._unit}")
        return UnitTimer(self, "inc", self._ns_per_time_unit)


# A summary that knows how to time in the specific unit
class TimingSummary(Summary, UnitTimable):
    def time(self) -> UnitTimer:
        if self._ns_per_time_unit is None:
            raise ValueError(f"Can't time in units of {self._unit}")
        return UnitTimer(self, "observe", self._ns_per_time_unit)


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
            reset_registry(registry)

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
    ) -> TimingCounter:
        return TimingCounter(
            name,
            documentation,
            unit=unit,
            labelnames=labelnames,
            namespace="ampel",
            subsystem=subsystem,
            registry=cls.registry(),
        )

    @classmethod
    def summary(
        cls, name, documentation, unit="", labelnames=(), subsystem=""
    ) -> Summary:
        return TimingSummary(
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
