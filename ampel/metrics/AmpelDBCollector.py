#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/metrics/AmpelDBCollector.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 11.02.2021
# Last Modified By  : jvs


from dataclasses import dataclass
from typing import List

from prometheus_client.metrics_core import GaugeMetricFamily, Metric # type: ignore

from ampel.core.AmpelDB import AmpelDB
from ampel.enum.DocumentCode import DocumentCode


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
                        {"code": DocumentCode.NEW}
                    ),
                )
            )
        except Exception:
            ...
        return metrics
