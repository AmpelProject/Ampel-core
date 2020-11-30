import asyncio

import pytest
from httpx import AsyncClient

from ampel.run import server


@pytest.fixture
def test_client(dev_context, monkeypatch):
    monkeypatch.setattr("ampel.run.server.context", dev_context)
    monkeypatch.setattr("ampel.run.server.task_manager.process_name_to_task", {})
    monkeypatch.setattr("ampel.run.server.task_manager.task_to_process_names", {})

    return AsyncClient(app=server.app, base_url="http://test")


@pytest.mark.asyncio
async def test_metrics(test_client):
    response = await test_client.get("/metrics")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_processes(test_client):
    response = await test_client.get("/processes")
    assert response.status_code == 200
    processes = response.json()["processes"]
    assert len(processes) == 0


@pytest.mark.asyncio
async def test_processes_start(test_client):
    dict.__setitem__(
        server.context.config._config["process"]["t2"],
        "sleepy",
        {
            "name": "sleepy",
            "schedule": "every(30).seconds",
            "tier": 2,
            "isolate": True,
            "controller": {
                "unit": "DefaultProcessController",
                "config": {"mp_join": 2},
            },
            "processor": {"unit": "Sleepy"},
        },
    )
    try:
        response = await test_client.post("/processes/start", params={"tier": 2})
        assert response.status_code == 200
        assert (await test_client.get("/tasks")).json() == response.json()
        tasks = response.json()["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["processes"] == ["sleepy"]
        assert len(tasks := list(server.task_manager.task_to_process_names.keys())) == 1
        await asyncio.wait_for(tasks[0], 5)
        assert not (await test_client.get("/tasks")).json()["tasks"]
    finally:
        await server.task_manager.shutdown()
