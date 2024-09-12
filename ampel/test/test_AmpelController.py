import asyncio

import pytest

from ampel.core.AmpelController import AmpelController


@pytest.mark.asyncio
async def test_run_cancel(dev_context):
    c = AmpelController(dev_context.config, tier=2)
    r = asyncio.create_task(c.run())
    await asyncio.sleep(0.1)
    r.cancel()
    await r

