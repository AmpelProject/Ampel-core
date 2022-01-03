#!/usr/bin/env python

import asyncio
import enum
import logging
import operator
import os
from datetime import datetime, timedelta
from functools import reduce
from typing import (
    Any,
    cast,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
)

from bson import json_util, tz_util, ObjectId
from fastapi import FastAPI, Header, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client.exposition import choose_encoder

from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AmpelController import AmpelController
from ampel.core.UnitLoader import UnitLoader
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.secret.AmpelVault import AmpelVault
from ampel.log.LogFlag import LogFlag
from ampel.metrics.AmpelDBCollector import AmpelDBCollector
from ampel.metrics.AmpelProcessCollector import AmpelProcessCollector
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.ProcessModel import ProcessModel
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelDB import AmpelDB
from ampel.enum.DocumentCode import DocumentCode
from ampel.util.hash import build_unsafe_dict_id

if TYPE_CHECKING:
    from ampel.protocol.LoggerProtocol import LoggerProtocol


class ProcessCollection(AmpelBaseModel):
    processes: list[ProcessModel]


class ProcessStatus(AmpelBaseModel):
    name: str
    tier: Literal[0, 1, 2, 3, None]
    status: Literal["running", "idle"]


class ProcessStatusCollection(AmpelBaseModel):
    processes: list[ProcessStatus]


class TaskDescription(AmpelBaseModel):
    id: str
    processes: list[str]


class TaskDescriptionCollection(AmpelBaseModel):
    tasks: list[TaskDescription]


app = FastAPI()
origins = [
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

log = logging.getLogger("ampel.run.server")
context: AmpelContext = None  # type: ignore[assignment]


class task_manager:
    process_name_to_controller_id: dict[str, str] = {}
    controller_id_to_task: dict[str, tuple[AbsProcessController, asyncio.Task]] = {}
    task_to_processes: dict[asyncio.Task, list[ProcessModel]] = {}

    @classmethod
    def get_task(cls, name: str):
        return cls.controller_id_to_task[cls.process_name_to_controller_id[name]][1]

    @classmethod
    def get_controller(cls, name: str):
        return cls.controller_id_to_task[cls.process_name_to_controller_id[name]][0]

    @classmethod
    async def add_processes(
        cls, processes: list[ProcessModel]
    ) -> TaskDescriptionCollection:
        """
        Add processes to the active set. If a process requests a controller
        config that was already instantiated, that controller will be updated
        with the process definition, replacing any definition that may exist
        under the same name. If the process requests a new controller
        configuration, a new task will be spawned.
        """
        global context
        groups: dict[str, list[ProcessModel]] = {}
        for pm in processes:
            if not pm.active:
                continue
            controller_id = build_unsafe_dict_id(
                pm.controller.dict(exclude_unset=True), ret=str
            )
            if controller_id in groups:
                groups[controller_id].append(pm)
            else:
                groups[controller_id] = [pm]
        for config_id, process_group in groups.items():
            names = [pm.name for pm in process_group]
            # update an existing controller
            if config_id in cls.controller_id_to_task:
                controller, task = cls.controller_id_to_task[config_id]
                # add the controller's existing processes to this group
                for pm in cls.task_to_processes[task]:
                    if pm.name not in names:
                        process_group.append(pm)
                controller.update(context.config, context.loader.vault, process_group)
                # update processes for this task
                cls.task_to_processes[task] = process_group
                # add updated processes to controller mapping
                for name in names:
                    cls.process_name_to_controller_id[name] = config_id
                log.info(
                    f"Updated task {id(task)} ({type(controller).__name__} {[pm.name for pm in process_group]})"
                )
            # spawn a new controller
            else:
                controller = context.loader.new(
                    process_group[0].controller,
                    unit_type=AbsProcessController,
                    config=context.config,
                    vault=context.loader.vault,
                    processes=process_group,
                )
                task = asyncio.create_task(controller.run())
                task.add_done_callback(cls.finalize_task)
                cls.task_to_processes[task] = process_group
                cls.controller_id_to_task[config_id] = (controller, task)
                for pm in process_group:
                    cls.process_name_to_controller_id[pm.name] = config_id
                log.info(
                    f"Launched task {id(task)} ({type(controller).__name__} {[pm.name for pm in process_group]})"
                )
        return TaskDescriptionCollection(
            tasks=[
                TaskDescription(
                    id=config_id,
                    processes=[pm.name for pm in cls.task_to_processes[task]],
                )
                for config_id, (controller, task) in cls.controller_id_to_task.items()
            ]
        )

    @classmethod
    async def remove_processes(cls, names: set[str]) -> None:
        """
        Remove the named process from the active set.
        """
        global context
        to_remove: dict[str, list[str]] = {}
        for name in names:
            if (config_id := cls.process_name_to_controller_id.get(name)) is None:
                continue
            if config_id in to_remove:
                to_remove[config_id].append(name)
            else:
                to_remove[config_id] = [name]
        expiring = set()
        for config_id, remove_group in to_remove.items():
            controller, task = cls.controller_id_to_task[config_id]
            keep: list[ProcessModel] = []
            drop: list[ProcessModel] = []
            for pm in cls.task_to_processes[task]:
                [keep, drop][pm.name in remove_group].append(pm)
            if not keep:
                controller.stop()
                # controller is empty; wait for it to exit
                expiring.add(task)
                log.info(
                    f"Stopping task {id(task)} ({type(controller).__name__} (empty))"
                )
            else:
                controller.update(context.config, context.loader.vault, keep)
                # controller still has assigned processes; clean up others
                for pm in drop:
                    cls.process_name_to_controller_id.pop(pm.name)
                cls.task_to_processes[task] = keep
                log.info(
                    f"Removed {[pm.name for pm in drop]} from task {id(task)} ({type(controller).__name__})"
                )
        await asyncio.gather(*expiring)

    @classmethod
    def finalize_task(cls, task: asyncio.Task) -> None:
        log.info(
            f"Task {id(task)} finished ({[pm.name for pm in cls.task_to_processes[task]]})"
        )
        for pm in cls.task_to_processes.pop(task):
            config_id = cls.process_name_to_controller_id.pop(pm.name)
            cls.controller_id_to_task.pop(config_id, None)

    @classmethod
    def get_status(cls, name: str) -> Literal["running", "idle"]:
        if name in cls.process_name_to_controller_id:
            return "running"
        else:
            return "idle"

    @classmethod
    async def shutdown(cls) -> None:
        tasks = cls.task_to_processes.keys()
        # Allow 10 seconds to stop gracefully
        for controller, _ in cls.controller_id_to_task.values():
            controller.stop()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 10)
        # Cancel any tasks that do not exit in time
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@app.on_event("startup")
async def init():
    if type(asyncio.get_event_loop()).__module__ == "uvloop":
        raise RuntimeError(
            "uvloop does not work with OS pipes (https://github.com/MagicStack/uvloop/issues/317). start uvicorn with --loop asyncio."
        )
    global context
    context = AmpelContext.load(
        os.environ.get("AMPEL_CONFIG", "config.yml"),
        vault=AmpelVault(providers=[DictSecretProvider.load(os.environ["AMPEL_SECRETS"])])
        if "AMPEL_SECRETS" in os.environ
        else None,
        freeze_config=False,
    )
    AmpelMetricsRegistry.register_collector(AmpelDBCollector(context.db))
    AmpelMetricsRegistry.register_collector(AmpelProcessCollector(name="server"))


app.on_event("shutdown")(task_manager.shutdown)


@app.post("/config/reload")
async def reload_config() -> TaskDescriptionCollection:
    # NB: async to prevent this running a thread
    config_file = os.environ.get("AMPEL_CONFIG", "config.yml")
    secrets_file = os.environ.get("AMPEL_SECRETS")
    try:
        logging.info(f"Reloading config from {config_file}")
        config = AmpelConfig.load(config_file, freeze=False)
        vault = AmpelVault(providers=[DictSecretProvider.load(secrets_file)]) if secrets_file else None
        db = AmpelDB.new(config, vault)
        loader = UnitLoader(config, db=db, vault=vault)
        # Ensure that process models are valid
        with loader.validate_unit_models():
            processes = AmpelController.get_processes(
                config, raise_exc=True, logger=cast("LoggerProtocol", log)
            )
    except Exception:
        logging.exception(f"Failed to load {config_file}")
        raise HTTPException(
            status_code=500, detail="Failed to reload configuration file"
        )

    # remove processes that are no longer defined or no longer in the active set
    await task_manager.remove_processes(
        set(task_manager.process_name_to_controller_id.keys()).difference(
            [pm.name for pm in processes]
        )
    )

    # update global context
    global context
    context = AmpelContext(config, db, loader)

    # update processes currently in the active set
    await task_manager.add_processes(processes)

    return await get_tasks()


# -------------------------------------
# Metrics
# -------------------------------------


@app.get("/metrics")
async def get_metrics(accept: None | str = Header(None)):
    encoder, content_type = choose_encoder(accept)
    return Response(content=encoder(AmpelMetricsRegistry), media_type=content_type)


# -------------------------------------
# Processes
# -------------------------------------


@app.get("/processes")
async def get_processes(
    tier: None | int = Query(None, ge=0, le=3, description="tier to include"),
    name: None | list[str] = Query(None),
    include: None | list[str] = Query(
        None, description="include processes with names that match"
    ),
    exclude: None | list[str] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: None | list[str] = Query(
        None, description="include processes with these controllers"
    ),
) -> ProcessCollection:
    processes = AmpelController.get_processes(
        context.config,
        tier=cast(Literal[0, 1, 2, 3], tier),
        match=include,
        exclude=exclude,
        controllers=controllers,
    )
    if name:
        processes = [pm for pm in processes if pm.name in name]
    return ProcessCollection(processes=processes)


@app.get("/processes/status")
async def get_processes_status(
    tier: None | int = Query(None, ge=0, le=3, description="tier to include"),
    name: None | list[str] = Query(None),
    include: None | list[str] = Query(
        None, description="include processes with names that match"
    ),
    exclude: None | list[str] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: None | list[str] = Query(
        None, description="include processes with these controllers"
    ),
) -> ProcessStatusCollection:
    processes = (
        await get_processes(tier, name, include, exclude, controllers)
    ).processes
    return ProcessStatusCollection(
        processes=[
            ProcessStatus(
                name=pm.name, tier=pm.tier, status=task_manager.get_status(pm.name)
            )
            for pm in processes
        ]
    )


@app.post("/processes/start")
async def start_processes(
    tier: None | int = Query(None, ge=0, le=3, description="tier to include"),
    name: None | list[str] = Query(None),
    include: None | list[str] = Query(
        None, description="include processes with names that match"
    ),
    exclude: None | list[str] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: None | list[str] = Query(
        None, description="include processes with these controllers"
    ),
) -> TaskDescriptionCollection:
    processes = (
        await get_processes(tier, name, include, exclude, controllers)
    ).processes
    return await task_manager.add_processes(processes)


@app.post("/processes/stop")
async def stop_processes(
    tier: None | int = Query(None, ge=0, le=3, description="tier to include"),
    name: None | list[str] = Query(None),
    include: None | list[str] = Query(
        None, description="include processes with names that match"
    ),
    exclude: None | list[str] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: None | list[str] = Query(
        None, description="include processes with these controllers"
    ),
) -> TaskDescriptionCollection:
    processes = (
        await get_processes(tier, name, include, exclude, controllers)
    ).processes
    await task_manager.remove_processes({pm.name for pm in processes})
    return await get_tasks()


@app.get("/tasks")
async def get_tasks() -> TaskDescriptionCollection:
    return TaskDescriptionCollection(
        tasks=[
            TaskDescription(
                id=config_id,
                processes=[pm.name for pm in task_manager.task_to_processes[task]],
            )
            for config_id, (
                controller,
                task,
            ) in task_manager.controller_id_to_task.items()
        ]
    )


@app.get("/process/{process}")
async def get_process(process: str) -> ProcessModel:
    for tier in range(4):
        try:
            doc = context.config.get(f"process.t{tier}.{process}", dict, raise_exc=True)
        except Exception:
            continue
        return ProcessModel(**doc)
    else:
        raise HTTPException(status_code=404, detail=f"{process} not found")


@app.post("/process/{process}/start")
async def start_process(process: str) -> TaskDescriptionCollection:
    processes = (
        await get_processes(
            tier=None, name=[process], include=None, exclude=None, controllers=None
        )
    ).processes
    return await task_manager.add_processes(processes)


@app.post("/process/{process}/stop")
async def stop_process(process: str):
    await task_manager.remove_processes({process})


@app.post("/process/{process}/kill")
async def kill_process(process: str):
    try:
        task = task_manager.get_controller(process)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"{process} is not running")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        ...


@app.post("/process/{process}/scale")
async def scale_process(
    process: str, multiplier: int = Query(..., gt=0, description="number of replicas")
) -> None:
    try:
        controller = task_manager.get_controller(process)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"{process} is not running")
    if hasattr(controller, "scale"):
        controller.scale(multiplier=multiplier)  # type: ignore
    else:
        raise HTTPException(
            status_code=405, detail=f"{type(controller).__name__} does not scale"
        )


# -------------------------------------
# DB data
# -------------------------------------


@app.get("/stock/{stock_id}")
def get_stock(stock_id: int):
    doc = json_util._json_convert(
        context.db.get_collection("stock").find_one({"_id": stock_id}),
        json_util.RELAXED_JSON_OPTIONS,
    )
    for k in "created", "updated":
        doc[k] = {facet: datetime.fromtimestamp(ts) for facet, ts in doc[k].items()}
    for jentry in doc["journal"]:
        jentry["ts"] = datetime.fromtimestamp(jentry["ts"])
    return doc


@app.get("/summary/channels")
def stock_summary():
    cursor = context.db.get_collection("stock").aggregate(
        [
            {
                "$facet": {
                    "channels": [
                        {"$unwind": "$channel"},
                        {
                            "$group": {
                                "_id": "$channel",
                                "count": {"$sum": 1},
                            }
                        },
                    ],
                    "total": [{"$count": "count"}],
                }
            }
        ]
    )
    facets = next(cursor)
    return {
        "channels": {
            doc["_id"]: doc["count"]
            for doc in sorted(facets["channels"], key=lambda doc: doc["_id"])
        },
        "total": facets["total"][0]["count"],
    }


def transform_doc(doc: dict[str, Any], tier: int) -> dict[str, Any]:
    doc = json_util._json_convert(doc, json_util.RELAXED_JSON_OPTIONS)
    if tier == 1:
        doc["added"] = datetime.fromtimestamp(doc["added"])
    if tier == 2:
        for jentry in doc["journal"]:
            jentry["dt"] = datetime.fromtimestamp(jentry["dt"])
        doc["code"] = DocumentCode(doc["code"]).name
        for subrecord in doc.get("body", []):
            subrecord["ts"] = datetime.fromtimestamp(subrecord["ts"])
    return doc


@app.get("/stock/{stock_id}/t{tier}")
def get_tier_docs(stock_id: int, tier: int = Query(..., ge=0, le=2)):
    cursor = context.db.get_collection(f"t{tier}").find({"stock": stock_id})
    return {
        "matches": [transform_doc(doc, tier) for doc in cursor for doc in cursor],
        "tier": tier,
    }


@app.get("/t2/summary")
def t2_summary():
    cursor = context.db.get_collection("t2").aggregate(
        [
            {
                "$group": {
                    "_id": {"code": "$code", "unit": "$unit"},
                    "count": {"$sum": 1},
                }
            }
        ]
    )
    summary = {}
    for doc in cursor:
        code = DocumentCode(doc["_id"]["code"]).name
        if code not in summary:
            summary[code] = {}
        summary[code][doc["_id"]["unit"]] = doc["count"]
    return summary


# -------------------------------------
# DB var
# -------------------------------------

# abbrevations used by DBLoggingHandler and FilterBlocksHandler
FIELD_ABBREV = {
    "a": "alert",
    "ac": "autocomplete",
    "c": "channel",
    "f": "flags",
    "m": "message",
    "s": "stock",
    "x": "extra",
}


async def query_time(
    after: None | timedelta | datetime = Query(None),
    before: None | timedelta | datetime = Query(None),
) -> list[dict[str, Any]]:
    andlist = []
    if after or before:
        now = datetime.now(tz_util.utc)
        if after:
            andlist.append(
                {
                    "_id": {
                        "$gt": ObjectId.from_datetime(
                            now - after if isinstance(after, timedelta) else after
                        )
                    }
                }
            )
        if before:
            andlist.append(
                {
                    "_id": {
                        "$lt": ObjectId.from_datetime(
                            now - before if isinstance(before, timedelta) else before
                        )
                    }
                }
            )
    return andlist


async def query_event(
    process: None | str = Query(None),
    tier: None | int = Query(None, ge=0, le=3, description="tier to include"),
    time_constraint: list[dict[str, Any]] = Depends(query_time),
) -> dict[str, Any]:
    query: dict[str, Any] = {}
    if process:
        query["process"] = process
    if tier is not None:
        query["tier"] = tier
    if time_constraint:
        query["$and"] = time_constraint
    return query


# NB: handlers that use Mongo are synchronous; FastAPI implicitly runs them
# in a thread. Could also use Motor for this.
@app.get("/events")
@app.get("/events/{process}")
def get_events(base_query: dict = Depends(query_event)):
    cursor = context.db.get_collection("events").find(
        {"run": {"$exists": True}, **base_query}
    )
    return {
        "events": [
            {
                "timestamp": doc["_id"].generation_time,
                **{
                    FIELD_ABBREV.get(k, k): v
                    for k, v in doc.items()
                    if k not in {"_id"}
                },
            }
            for doc in cursor
        ]
    }


@app.get("/logs/{run_id}")
def get_logs(
    run_id: int,
    flags: None | list[enum.Enum("LogFlagName", {k: k for k in LogFlag.__members__})] = Query(None), # type: ignore[valid-type]
):
    query: dict[str, Any] = {"r": run_id}
    if flags:
        query["f"] = {
            "$bitsAllSet": reduce(
                operator.or_, [LogFlag.__members__[k.name] for k in flags]
            )
        }
    cursor = context.db.get_collection("logs").find(query, {"r": 0})
    translate_keys = {"_id", "f"}
    return {
        "logs": [
            {
                "timestamp": doc["_id"].generation_time,
                "flags": [
                    k for k, v in LogFlag.__members__.items() if (v & doc["f"])
                ],
                **{
                    FIELD_ABBREV.get(k, k): v
                    for k, v in doc.items()
                    if k not in translate_keys
                },
            }
            for doc in cursor
        ]
    }


@app.get("/troubles")
def get_troubles(base_query: dict = Depends(query_event)):
    cursor = context.db.get_collection("troubles").find(base_query)
    return {
        "troubles": [
            {
                "timestamp": doc["_id"].generation_time,
                **{k: v for k, v in doc.items() if k not in {"_id"}},
            }
            for doc in cursor
        ]
    }


if __name__ == "__main__":
    import uvicorn  # type: ignore

    # NB: libuv does not play nice with OS pipes, so concurrent.process will
    # not work with uvloop: https://github.com/MagicStack/uvloop/issues/317
    uvicorn.run(
        "ampel.run.server:app",
        host="127.0.0.1",
        port=5000,
        log_level="info",
        loop="asyncio",
    )
