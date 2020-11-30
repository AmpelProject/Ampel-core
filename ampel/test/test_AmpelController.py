import asyncio

import pytest

from ampel.core.AmpelController import AmpelController
from prometheus_client import make_asgi_app

from httpx import AsyncClient

@pytest.mark.asyncio
async def test_run_cancel(dev_context):
    c = AmpelController(dev_context.config, tier=2)
    r = asyncio.create_task(c.run())
    await asyncio.sleep(0.1)
    r.cancel()
    await r

@pytest.mark.asyncio
async def test_metrics():
    async with AsyncClient(app=make_asgi_app(), base_url="http://test") as ac:
        response = await ac.get("/")
        assert response.headers["content-type"] == "text/plain"
        assert response.status_code == 200

