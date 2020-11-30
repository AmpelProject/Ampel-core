import pytest

from ampel.run.server import app
from httpx import AsyncClient

@pytest.fixture
def test_client(dev_context, monkeypatch):
    monkeypatch.setattr("ampel.run.server.context", dev_context)
    return AsyncClient(app=app, base_url="http://test")

@pytest.mark.asyncio
async def test_metrics(test_client):
    response = await test_client.get("/metrics")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_processes(test_client):
    response = await test_client.get("/processes")
    assert response.status_code == 200
    processes = response.json()['processes']
    assert len(processes) == 0
