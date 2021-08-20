import asyncio
from contextlib import asynccontextmanager

import pytest

from ampel.core.DefaultProcessController import DefaultProcessController
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry


@asynccontextmanager
async def run(controller):
    task = asyncio.create_task(controller.run())
    try:
        yield task
    finally:
        controller.stop()
        await task


@pytest.mark.asyncio
async def test_process_gauge(dev_context, ampel_logger):

    process_count = lambda: AmpelMetricsRegistry.registry().get_sample_value(
        "ampel_processes", {"tier": "2", "process": "sleepy"}
    )

    c = DefaultProcessController(
        config=dev_context.config,
        processes=[
            {
                "name": "sleepy",
                "schedule": "every(30).seconds",
                "tier": 2,
                "isolate": True,
                "version": 0,
                "processor": {"unit": "Sleepy"},
            }
        ],
    )
    async with run(c) as task:
        try:
            await asyncio.wait_for(asyncio.shield(task), 1)
        except asyncio.exceptions.TimeoutError:
            ...
        assert process_count() == 1
    assert process_count() == 0
