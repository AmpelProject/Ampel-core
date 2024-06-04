#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/metrics/AmpelDBCollector.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  11.02.2021
# Last Modified By:    jvs


from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass

from prometheus_client.metrics_core import GaugeMetricFamily, Metric

from ampel.core.AmpelDB import AmpelDB
from ampel.enum.DocumentCode import DocumentCode


@dataclass
class AmpelDBCollector:
    """
    Collect various metrics from the Ampel DB collections. These are quantities
    that are not implicitly collected by any worker process.
    """

    db: AmpelDB

    def collect(self) -> Sequence[Metric]:

        metrics = []
        with suppress(Exception):
            metrics.append(
                GaugeMetricFamily(
                    "ampel_t2_docs_queued",
                    "Number of T2 docs awaiting processing",
                    value=self.db.get_collection("t2").count_documents(
                        {"code": DocumentCode.NEW}
                    ),
                )
            )
        return metrics
