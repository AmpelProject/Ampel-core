from contextlib import suppress
from dataclasses import dataclass

import psutil
from prometheus_client.metrics_core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    Metric,
)

from ampel.util.concurrent import _Process


@dataclass
class AmpelProcessCollector:
    """
    Collect current resource usage of subprocesses launched via
    ampel.util.concurrent
    """

    #: Name to use for main process. If None, do not collect process metrics for main process.
    name: None | str = None

    def get_pids(self) -> list[tuple[tuple[str, str], None | int]]:
        """
        Collect tuples of (labels, pid) for subprocesses, and, optionally, this process
        """
        processes: list[tuple[tuple[str, str], None | int]] = []
        for name, replicas in _Process._active.items():  # noqa: SLF001
            for replica, pid in replicas.items():
                processes.append(((name, str(replica)), pid))
        if self.name:
            processes.append(((self.name, "0"), None))
        return processes

    def collect(self) -> list[Metric]:

        rss = GaugeMetricFamily(
            "ampel_resident_memory_bytes",
            "Resident memory size in bytes.",
            labels=("process", "replica"),
        )
        cpu = CounterMetricFamily(
            "ampel_cpu_seconds_total",
            "Total user and system CPU time spent in seconds.",
            labels=("process", "replica"),
        )

        for labels, pid in self.get_pids():
            with suppress(psutil.NoSuchProcess):
                p = psutil.Process(pid)
                with p.oneshot():
                    rss.add_metric(labels, p.memory_info().rss)
                    cpu.add_metric(
                        labels, (p.cpu_times().user + p.cpu_times().user)
                    )
        return [rss, cpu]
