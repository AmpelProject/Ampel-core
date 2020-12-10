from dataclasses import dataclass
from typing import List

from prometheus_client.metrics_core import GaugeMetricFamily, Metric # type: ignore

from ampel.db.AmpelDB import AmpelDB
from ampel.t2.T2RunState import T2RunState


@dataclass
class AmpelDBCollector:
    """
    Collect various metrics from the Ampel DB collections. These are quantities
    that are not implicitly collected by any worker process.
    """

    db: AmpelDB

    def collect(self) -> List[Metric]:

        metrics = []
        try:
            metrics.append(
                GaugeMetricFamily(
                    "ampel_t2_docs_queued",
                    "Number of T2 docs awaiting processing",
                    value=self.db.get_collection("t2").count_documents(
                        {"status": T2RunState.TO_RUN}
                    ),
                )
            )
        except:
            ...
        return metrics
