#
# Copyright (c) 2015-2018 Canonical, Ltd.
#
# This file is part of Talisker
# (see http://github.com/canonical-ols/talisker).
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Modifications for Ampel:
# - removed Python <= 3.8 compat + dependencies on talisker itself
# - removed locks (only ever called from one process)
# - added implicit per-worker labels

# -*- coding: utf-8 -*-

import glob
import os
import tempfile

from prometheus_client import (
    CollectorRegistry,
    core,
    generate_latest,
    mmap_dict,
    multiprocess,
)

histogram_archive = "histogram_archive.db"
counter_archive = "counter_archive.db"


def collect_metrics():

    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    else:
        registry = core.REGISTRY
    return generate_latest(registry)


def prometheus_setup_worker(labels=None):
    """
    Monkey-patch mmap_key and ValueClass to add implicit labels. This must be
    done before any metrics are instantiated.
    """
    if labels:
        from prometheus_client import values

        def mmap_key(metric_name, name, labelnames, labelvalues):
            return mmap_dict.mmap_key(
                metric_name,
                name,
                tuple(labels.keys()) + labelnames,
                tuple(labels.values()) + labelvalues,
            )

        values.mmap_key = mmap_key
        # synthesize a new ValueClass (captures mmap_key)
        values.ValueClass = values.get_value_class()


def prometheus_cleanup_worker(pid):
    """
    Aggregate dead worker's metrics into a single archive file, preventing
    collection time from growing without bound as pointed out in
    - https://github.com/prometheus/client_python/issues/568
    - https://github.com/prometheus/client_python/issues/443
    - https://github.com/prometheus/client_python/pull/430
    - https://github.com/prometheus/client_python/pull/441
    """

    multiprocess.mark_process_dead(pid)  # this takes care of gauges
    prom_dir = os.environ["PROMETHEUS_MULTIPROC_DIR"]
    # also remove max gauges
    for f in glob.glob(os.path.join(prom_dir, f"gauge_max_{pid}.db")):
        os.remove(f)

    # check at least one worker file exists
    if not (
        paths := [
            worker_file
            for kind in ("histogram", "counter")
            if os.path.exists(worker_file := os.path.join(prom_dir, f"{kind}_{pid}.db"))
        ]
    ):
        return

    histogram_path = os.path.join(prom_dir, histogram_archive)
    counter_path = os.path.join(prom_dir, counter_archive)
    archive_paths = [p for p in [histogram_path, counter_path] if os.path.exists(p)]

    collect_paths = paths + archive_paths
    collector = multiprocess.MultiProcessCollector(None)

    metrics = collector.merge(collect_paths, accumulate=False)

    tmp_histogram = tempfile.NamedTemporaryFile(delete=False)
    tmp_counter = tempfile.NamedTemporaryFile(delete=False)
    write_metrics(metrics, tmp_histogram.name, tmp_counter.name)

    # no lock here, since this is only ever called from the asyncio event loop
    # of a single process
    os.rename(tmp_histogram.name, histogram_path)
    os.rename(tmp_counter.name, counter_path)

    for path in paths:
        os.unlink(path)


def write_metrics(metrics, histogram_file, counter_file):

    histograms = mmap_dict.MmapedDict(histogram_file)
    counters = mmap_dict.MmapedDict(counter_file)

    try:
        for metric in metrics:
            if metric.type == "histogram":
                sink = histograms
            elif metric.type == "counter":
                sink = counters
            else:
                continue

            for sample in metric.samples:
                # prometheus_client 0.4+ adds extra fields
                name, labels, value = sample[:3]
                key = mmap_dict.mmap_key(
                    metric.name, name, tuple(labels), tuple(labels.values()),
                )
                sink.write_value(key, value)
    finally:
        histograms.close()
        counters.close()
