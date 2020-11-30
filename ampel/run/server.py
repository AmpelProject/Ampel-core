#!/usr/bin/env python

import asyncio
import enum
import logging
import operator
import os
from datetime import datetime
from functools import reduce
from typing import Any, cast, Dict, List, Literal, Optional, Tuple

from bson import json_util, ObjectId
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client.exposition import choose_encoder

from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AmpelController import AmpelController
from ampel.dev.DictSecretProvider import DictSecretProvider
from ampel.log.LogRecordFlag import LogRecordFlag
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.ProcessModel import ProcessModel
from ampel.model.StrictModel import StrictModel
from ampel.t2.T2RunState import T2RunState
from ampel.util.mappings import build_unsafe_dict_id


class ProcessCollection(StrictModel):
    processes: List[ProcessModel]


class ProcessStatus(StrictModel):
    name: str
    tier: Literal[0, 1, 2, 3, "ops"]
    status: Literal["running", "idle"]


class ProcessStatusCollection(StrictModel):
    processes: List[ProcessStatus]


class TaskDescription(StrictModel):
    id: int
    processes: List[str]


class TaskDescriptionCollection(StrictModel):
    tasks: List[TaskDescription]


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
    process_name_to_task: Dict[str, Tuple[AbsProcessController, asyncio.Task]] = {}
    task_to_process_names: Dict[asyncio.Task, List[str]] = {}

    @classmethod
    def start_controller(cls, controller: AbsProcessController) -> asyncio.Task:
        names = [pm.name for pm in controller.processes]
        for name in names:
            assert name not in cls.process_name_to_task, "process not currently running"
        task = asyncio.create_task(controller.run())
        cls.task_to_process_names[task] = names
        for name in names:
            cls.process_name_to_task[name] = (controller, task)
        task.add_done_callback(cls.finalize_task)
        log.info(f"Launched task {id(task)} ({type(controller).__name__} {names})")
        return task

    @classmethod
    def finalize_task(cls, task: asyncio.Task) -> None:
        log.info(f"Task {id(task)} finished ({cls.task_to_process_names[task]})")
        for name in cls.task_to_process_names.pop(task):
            cls.process_name_to_task.pop(name)

    @classmethod
    def get_status(cls, name: str) -> Literal["running", "idle"]:
        if name in cls.process_name_to_task:
            return "running"
        else:
            return "idle"

    @classmethod
    async def shutdown(cls) -> None:
        # FIXME: implement soft shutdown
        tasks = cls.task_to_process_names.keys()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@app.on_event("startup")
async def init():
    global context
    context = AmpelContext.load(
        os.environ.get("AMPEL_CONFIG", "config.yml"),
        secrets=DictSecretProvider.load(os.environ["AMPEL_SECRETS"])
        if "AMPEL_SECRETS" in os.environ
        else None,
        freeze_config=False,
    )


app.on_event("shutdown")(task_manager.shutdown)


# -------------------------------------
# Metrics
# -------------------------------------


@app.get("/metrics")
async def get_metrics(accept: Optional[str] = Header(None)):
    encoder, content_type = choose_encoder(accept)
    return Response(
        content=encoder(AmpelMetricsRegistry.registry()), media_type=content_type
    )


# -------------------------------------
# Processes
# -------------------------------------


@app.get("/processes")
async def get_processes(
    tier: Optional[int] = Query(None, ge=0, le=3, description="tier to include"),
    name: Optional[List[str]] = Query(None),
    include: Optional[List[str]] = Query(
        None, description="include processes with names that match"
    ),
    exclude: Optional[List[str]] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: Optional[List[str]] = Query(
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
    tier: Optional[int] = Query(None, ge=0, le=3, description="tier to include"),
    name: Optional[List[str]] = Query(None),
    include: Optional[List[str]] = Query(
        None, description="include processes with names that match"
    ),
    exclude: Optional[List[str]] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: Optional[List[str]] = Query(
        None, description="include processes with these controllers"
    ),
) -> ProcessStatusCollection:
    processes = (
        await get_processes(tier, name, include, exclude, controllers)
    ).processes
    return ProcessStatusCollection(
        processes=[
            ProcessStatus(
                **{
                    "name": pm.name,
                    "tier": pm.tier,
                    "status": task_manager.get_status(pm.name),
                }
            )
            for pm in processes
        ]
    )


def group_processes_by_controller(
    processes: List[ProcessModel],
) -> Dict[int, List[ProcessModel]]:
    groups: Dict[int, List[ProcessModel]] = {}
    for pm in processes:
        controller_id = build_unsafe_dict_id(
            pm.controller.dict(exclude_none=True), ret=int
        )
        if controller_id in groups:
            groups[controller_id].append(pm)
        else:
            groups[controller_id] = [pm]

    return groups


def create_controllers(
    context: AmpelContext, processes: List[ProcessModel]
) -> List[AbsProcessController]:
    return [
        context.loader.new(
            process_group[0].controller,
            unit_type=AbsProcessController,
            config=context.config,
            secrets=context.loader.secrets,
            processes=process_group,
        )
        for process_group in group_processes_by_controller(processes).values()
    ]


@app.post("/processes/start")
async def start_processes(
    tier: Optional[int] = Query(None, ge=0, le=3, description="tier to include"),
    name: Optional[List[str]] = Query(None),
    include: Optional[List[str]] = Query(
        None, description="include processes with names that match"
    ),
    exclude: Optional[List[str]] = Query(
        None, description="exclude processes with names that match"
    ),
    controllers: Optional[List[str]] = Query(
        None, description="include processes with these controllers"
    ),
) -> TaskDescriptionCollection:
    assert context
    processes = (
        await get_processes(tier, name, include, exclude, controllers)
    ).processes
    tasks = []
    for controller in create_controllers(context, processes):
        task = task_manager.start_controller(controller)
        tasks.append(
            TaskDescription(
                id=id(task), processes=[pm.name for pm in controller.processes]
            )
        )
    return TaskDescriptionCollection(tasks=tasks)


@app.get("/tasks")
async def get_tasks() -> TaskDescriptionCollection:
    return TaskDescriptionCollection(
        tasks=[
            TaskDescription(id=id(task), processes=processes)
            for task, processes in task_manager.task_to_process_names.items()
        ]
    )


@app.get("/process/{process}")
async def get_process(process: str) -> ProcessModel:
    for tier in range(4):
        try:
            doc = context.config.get(f"process.t{tier}.{process}", dict, raise_exc=True)
        except:
            continue
        return ProcessModel(**doc)
    else:
        raise HTTPException(status_code=404, detail=f"{process} not found")


@app.post("/process/{process}/start")
async def start_process(process: str) -> TaskDescriptionCollection:
    return await start_processes(name=[process])


@app.post("/process/{process}/stop")
async def stop_process(process: str):
    try:
        controller, task = task_manager.process_name_to_task[process]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"{process} is not running")
    controller.stop()
    await task.result()


@app.post("/process/{process}/kill")
async def kill_process(process: str):
    try:
        controller, task = task_manager.process_name_to_task[process]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"{process} is not running")
    task.cancel()
    try:
        await task.result()
    except asyncio.CancelledError:
        ...


# -------------------------------------
# DB data
# -------------------------------------


@app.get("/stock/{stock_id}")
def get_stock(stock_id: int):
    doc = context.db.get_collection("stock").find_one({"_id": stock_id})
    return json_util._json_convert(doc, json_util.RELAXED_JSON_OPTIONS)


@app.get("/summary/channels")
def stock_summary():
    cursor = context.db.get_collection("stock").aggregate(
        [
            {
                "$facet": {
                    "channels": [
                        {"$unwind": "$channel"},
                        {"$group": {"_id": "$channel", "count": {"$sum": 1},}},
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


@app.get("/stock/{stock_id}/t{tier}")
def get_tier_docs(stock_id: int, tier: int = Query(..., ge=0, le=2)):
    cursor = context.db.get_collection(f"t{tier}").find({"stock": stock_id})
    return {
        "matches": [
            json_util._json_convert(doc, json_util.RELAXED_JSON_OPTIONS)
            for doc in cursor
        ],
        "tier": tier,
    }


@app.get("/t2/summary")
def t2_summary():
    cursor = context.db.get_collection("t2").aggregate(
        [
            {
                "$group": {
                    "_id": {"status": "$status", "unit": "$unit"},
                    "count": {"$sum": 1},
                }
            }
        ]
    )
    summary = {}
    for doc in cursor:
        status = T2RunState(doc["_id"]["status"]).name
        if not status in summary:
            summary[status] = {}
        summary[status][doc["_id"]["unit"]] = doc["count"]
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

# NB: handlers that use Mongo are synchronous; FastAPI implicitly runs them
# in a thread. Could also use Motor for this.
@app.get("/events")
@app.get("/events/{process}")
def get_events(process: Optional[str] = None):
    query: Dict[str, Any] = {"run": {"$exists": True}}
    if process:
        query["process"] = process
    cursor = context.db.get_collection("events").find(query)
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
    flags: Optional[  # type: ignore[valid-type]
        List[enum.Enum("LogRecordFlagName", {k: k for k in LogRecordFlag.__members__})]
    ] = Query(None),
):
    query: Dict[str, Any] = {"r": run_id}
    if flags:
        query["f"] = {
            "$bitsAllSet": reduce(
                operator.or_, [LogRecordFlag.__members__[k.name] for k in flags]
            )
        }
    cursor = context.db.get_collection("logs").find(query, {"r": 0})
    translate_keys = {"_id", "f"}
    return {
        "logs": [
            {
                "timestamp": doc["_id"].generation_time,
                "flags": [
                    k for k, v in LogRecordFlag.__members__.items() if (v & doc["f"])
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
def get_troubles(
    tier: Optional[int] = Query(None, ge=0, le=3, description="tier to include"),
    process: Optional[str] = None,
    after: Optional[datetime] = None,
    before: Optional[datetime] = None,
):
    query: Dict[str, Any] = {}
    if tier:
        query["tier"] = tier
    if process:
        query["process"] = process
    if after or before:
        andlist = []
        if after:
            andlist.append({"_id": {"$gt": ObjectId.from_datetime(after)}})
        if before:
            andlist.append({"_id": {"$lt": ObjectId.from_datetime(before)}})
        query["$and"] = andlist
    cursor = context.db.get_collection("troubles").find(query)
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
