import asyncio
from io import StringIO

import pytest
from httpx import AsyncClient
from prometheus_client.parser import text_fd_to_metric_families

from ampel.metrics.AmpelDBCollector import AmpelDBCollector
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.run import server
from ampel.t2.T2RunState import T2RunState


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


@pytest.fixture
def db_collector(dev_context):
    c = AmpelDBCollector(dev_context.db)
    AmpelMetricsRegistry.register_collector(c)
    yield
    AmpelMetricsRegistry.deregister_collector(c)


@pytest.mark.asyncio
async def test_db_metrics(test_client, db_collector, dev_context):
    async def check_metric(name, value):
        response = await test_client.get("/metrics")
        assert response.status_code == 200
        for metric in text_fd_to_metric_families(StringIO(response.text)):
            print(type(metric))
            if metric.name == name:
                assert len(metric.samples) == 1
                assert metric.samples[0].value == value
                break
        else:
            raise ValueError(f"metric {name} not collected")

    await check_metric("ampel_t2_docs_queued", 0)
    dev_context.db.get_collection("t2").insert_one({"state": T2RunState.TO_RUN})
    await check_metric("ampel_t2_docs_queued", 1)


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


@pytest.mark.asyncio
async def test_process_stop(test_client):
    dict.__setitem__(
        server.context.config._config["process"]["t2"],
        "sleepy",
        {
            "name": "sleepy",
            "schedule": "every(30).seconds",
            "tier": 2,
            "isolate": True,
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

        await asyncio.sleep(0.1)
        await test_client.post("/process/sleepy/stop")

        await asyncio.wait_for(tasks[0], 5)
        assert not (await test_client.get("/tasks")).json()["tasks"]
    finally:
        await server.task_manager.shutdown()
