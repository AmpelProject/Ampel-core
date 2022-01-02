#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/test/test_server.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  11.02.2021
# Last Modified By:    jvs

import asyncio, pytest, yaml
from datetime import datetime
from io import StringIO
from httpx import AsyncClient
from prometheus_client.parser import text_fd_to_metric_families

from ampel.metrics.AmpelDBCollector import AmpelDBCollector
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.run import server
from ampel.enum.DocumentCode import DocumentCode
from ampel.util.freeze import recursive_unfreeze
from ampel.util.mappings import set_by_path


@pytest.fixture
async def test_client(dev_context, monkeypatch):
    monkeypatch.setattr("ampel.run.server.context", dev_context)
    for attr in (
        "process_name_to_controller_id",
        "controller_id_to_task",
        "task_to_processes",
    ):
        monkeypatch.setattr(f"ampel.run.server.task_manager.{attr}", {})

    async with AsyncClient(app=server.app, base_url="http://test") as client:
        yield client


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
            if metric.name == name:
                assert len(metric.samples) == 1
                assert metric.samples[0].value == value
                break
        else:
            raise ValueError(f"metric {name} not collected")

    await check_metric("ampel_t2_docs_queued", 0)
    dev_context.db.get_collection("t2").insert_one({"code": DocumentCode.NEW})
    await check_metric("ampel_t2_docs_queued", 1)


@pytest.mark.asyncio
async def test_reload(test_client, dev_context, testing_config, monkeypatch):
    monkeypatch.setenv("AMPEL_CONFIG", str(testing_config))
    assert server.context is dev_context
    response = await test_client.post("/config/reload")
    assert response.status_code == 200
    assert server.context is not dev_context


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
            "version": 0,
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

        assert len(tasks := list(server.task_manager.task_to_processes.keys())) == 1
        await asyncio.wait_for(tasks[0], 5)
        assert not (await test_client.get("/tasks")).json()["tasks"]
    finally:
        await server.task_manager.shutdown()


@pytest.mark.parametrize(
    "patches,should_raise",
    [
        (None, False),
        ({"processor.config": {"nonexistant_param": True}}, True),
        ({"active": False, "processor.config": {"nonexistant_param": True}}, False),
    ],
)
@pytest.fixture
def config_in_env(monkeypatch, tmp_path, dev_context, patches, should_raise):
    config = recursive_unfreeze(dev_context.config.get())
    config["process"]["t3"]["sleepy"] = {
        "name": "sleepy",
        "schedule": "every(30).seconds",
        "tier": 3,
        "isolate": True,
        "controller": {
            "unit": "DefaultProcessController",
            "config": {"mp_join": 2},
        },
        "processor": {"unit": "Sleepy"},
    }
    if patches:
        for k, v in patches.items():
            set_by_path(config["process"]["t3"]["sleepy"], k, v)
    with open(tmp_path / "config.yaml", "w") as f:
        yaml.dump(config, f)
    monkeypatch.setenv("AMPEL_CONFIG", str(tmp_path / "config.yaml"))


@pytest.mark.parametrize(
    "patches,should_raise",
    [
        ({}, False),
        ({"processor.config": {"nonexistant_param": True}}, True),
        ({"active": False, "processor.config": {"nonexistant_param": True}}, False),
    ],
)
@pytest.mark.asyncio
async def test_config_reload(
    test_client, monkeypatch, tmp_path, dev_context, patches, should_raise, mocker
):

    config = recursive_unfreeze(dev_context.config.get())
    config["process"]["t3"]["sleepy"] = {
        "name": "sleepy",
        "schedule": "every(30).seconds",
        "tier": 3,
        "version": 0,
        "isolate": True,
        "controller": {
            "unit": "DefaultProcessController",
            "config": {"mp_join": 2},
        },
        "processor": {"unit": "Sleepy"},
    }
    for k, v in patches.items():
        set_by_path(config["process"]["t3"]["sleepy"], k, v)
    with open(tmp_path / "config.yaml", "w") as f:
        yaml.dump(config, f)
    monkeypatch.setenv("AMPEL_CONFIG", str(tmp_path / "config.yaml"))

    try:
        remove = mocker.patch("ampel.run.server.task_manager.remove_processes")
        add = mocker.patch("ampel.run.server.task_manager.add_processes")
        response = await test_client.post("/config/reload")
        if should_raise:
            assert response.status_code == 500
        else:
            response.raise_for_status()
            assert await remove.called_once()
            assert len(remove.call_args[0][0]) == 0
            assert await add.called_once()
            assert len(add.call_args[0][0]) == (1 if config["process"]["t3"]["sleepy"].get("active", True) else 0)
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
            "version": 0,
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

        assert len(tasks := list(server.task_manager.task_to_processes.keys())) == 1

        await asyncio.sleep(0.1)
        await test_client.post("/process/sleepy/stop")

        await asyncio.wait_for(tasks[0], 5)
        assert not (await test_client.get("/tasks")).json()["tasks"]
    finally:
        await server.task_manager.shutdown()


@pytest.mark.asyncio
async def test_event_query(test_client, mocker):
    m = mocker.patch("ampel.run.server.context.db")
    find = m.get_collection().find
    await test_client.get(
        "/events", params={"after": 7200, "process": "InfantSNSummary"}
    )
    assert isinstance(query := find.call_args.args[0], dict)
    assert isinstance(andlist := query["$and"], list)
    gtime = andlist[0]["_id"]["$gt"].generation_time
    assert 7200 < (datetime.now(gtime.tzinfo) - gtime).total_seconds() < 7230
