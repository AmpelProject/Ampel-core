#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/ProcessCommand.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  14.08.2022
# Last Modified By:    jvs

import json
import os
import signal
import traceback
from argparse import ArgumentParser
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from time import time
from typing import Any

import yaml

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.config.AmpelConfig import OutdatedConfigError
from ampel.cli.AbsCoreCommand import AbsCoreCommand, get_vault
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.core.UnitLoader import UnitLoader
from ampel.core.EventHandler import EventHandler
from ampel.core.AmpelContext import AmpelContext
from ampel.core.Schedulable import Schedulable
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from ampel.struct.Resource import Resource
from ampel.util.freeze import recursive_freeze
from ampel.util.template import apply_templates


def _handle_traceback(signal, frame):
    print(traceback.print_stack(frame))


help = {
    "run": "Execute the process",
    "validate": "Validate the process definition",
    "debug": "enable traceback printing",
    "handle-exc": "record exceptions in the db",
    "log-profile": "logging profile to use",
    "config": "path to an ampel config file (yaml/json)",
    "schema": "path to YAML job file",
    "name": "task name",
    "workflow": "parent workflow name",
    "secrets": "path to a secret store; either SOPS YAML or mounted k8s Secret directory",
    "db": "database to use",
    "channel": "path to YAML channel file",
    "alias": "path to YAML alias file",
}


class MockAmpelDB:
    def __init__(self) -> None:
        self.conf_ids: dict[int, dict[str, Any]] = {}

    def add_conf_id(self, confid: int, conf: dict[str, Any]):
        self.conf_ids[confid] = conf


class ProcessCommand(AbsCoreCommand):
    """
    Runs a single process from a task definition (YAML file)

    A process is a single step of a Job (see JobCommand), with all templates resolved.
    """

    @staticmethod
    def get_sub_ops() -> list[str]:
        return ["run", "validate"]

    # Mandatory implementation
    def get_parser(
        self, sub_op: None | str = None
    ) -> ArgumentParser | AmpelArgumentParser:

        if sub_op is None:
            sub_op = "run"
        if sub_op in self.parsers:
            return self.parsers[sub_op]

        sub_ops = self.get_sub_ops()
        if sub_op is None or sub_op not in sub_ops:
            return AmpelArgumentParser.build_choice_help(
                "process",
                sub_ops,
                help,
                description="Run a single process from a task definition (YAML file).",
            )

        builder = ArgParserBuilder("process")
        builder.add_parsers(sub_ops, help)
        builder.notation_add_note_references()
        builder.notation_add_example_references()

        builder.req("config")
        builder.req("schema")
        builder.req("name", "run")
        builder.opt("name", "validate")
        builder.req("db", "run", type=str)
        builder.opt("db", "validate", type=str)

        builder.opt("resources-in")
        builder.opt("resources-out")

        builder.opt("channel")
        builder.opt("alias")
        builder.opt("workflow", default=None, type=str)
        builder.opt("log-profile", default="prod")
        builder.opt("debug", default=False, action="store_true")
        builder.opt("handle-exc", default=False, action="store_true")
        builder.opt("secrets", type=str)

        # Example
        builder.example(
            "run",
            "-config ampel_conf.yaml schema task_file.yaml -db processing -name taskytask",
        )
        builder.example(
            "validate",
            "-config ampel_conf.yaml schema task_file.yaml",
        )
        self.parsers.update(builder.get())

        return self.parsers[sub_op]

    def _get_context(
        self,
        args: dict[str, Any],
        unknown_args: Sequence[str],
        sub_op: str | None,
        logger: AmpelLogger,
    ) -> AmpelContext:

        # DevAmpelContext hashes automatically confid from potential IngestDirectives
        if sub_op == "run":
            ctx = super().get_context(
                args,
                unknown_args,
                logger,
                freeze_config=False,
                ContextClass=DevAmpelContext,
                purge_db=False,
                db_prefix=args["db"],
                require_existing_db=False,
                one_db=True,
            )
        else:
            # Do not connect to the database for validation, just mock the conf_id store
            config = self.load_config(
                args["config"], unknown_args, logger, freeze=False
            )
            vault = get_vault(args)
            return AmpelContext(
                config=config,
                db=MockAmpelDB(),  # type: ignore[arg-type]
                loader=UnitLoader(config, db=None, vault=vault, provenance=False),
            )

        config_dict = ctx.config._config  # noqa: SLF001

        # load channels if provided
        if args["channel"]:
            with open(args["channel"]) as f:
                for c in yaml.safe_load(f):
                    chan = ChannelModel(**c)
                    logger.info(f"Registering job channel '{chan.channel}'")
                    dict.__setitem__(config_dict["channel"], str(chan.channel), c)

        # load custom aliases if provided
        if args["alias"]:
            with open(args["alias"]) as f:
                for k, v in yaml.safe_load(f).items():
                    if k not in ("t0", "t1", "t2", "t3"):
                        raise ValueError(f"Unrecognized alias: {k}")
                    if "alias" not in config_dict:
                        dict.__setitem__(config_dict, "alias", {})
                    for kk, vv in v.items():
                        logger.info(f"Registering job alias '{kk}'")
                        if k not in config_dict["alias"]:
                            dict.__setitem__(config_dict["alias"], k, {})
                        dict.__setitem__(config_dict["alias"][k], kk, vv)

        ctx.config._config = recursive_freeze(config_dict)  # noqa: SLF001

        return ctx

    @contextmanager
    def push_metrics(self, process_name: str, logger: AmpelLogger) -> Generator:
        if not (pushgateway := os.environ.get("PROMETHEUS_PUSHGATEWAY")):
            yield
            return

        task = Schedulable()
        task.get_scheduler().every(30).seconds.do(
            AmpelMetricsRegistry.push, pushgateway, process_name, reset=True
        )
        with task.run_in_thread():
            yield
        try:
            task.get_scheduler().run_all()
        except Exception as exc:
            logger.error("Failed to push metrics", exc_info=exc)

    def run(
        self,
        args: dict[str, Any],
        unknown_args: Sequence[str],
        sub_op: None | str = None,
    ) -> int | None:

        if args["debug"]:
            signal.signal(signal.SIGUSR1, _handle_traceback)

        start_time = time()
        logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

        try:
            ctx = self._get_context(
                args,
                unknown_args,
                sub_op or "run",
                logger=logger,
            )
        except OutdatedConfigError:
            return 1

        with open(args["schema"]) as f:
            taskd = yaml.safe_load(f)
            if "template" in taskd:
                taskd = apply_templates(ctx, taskd["template"], taskd, logger)
                taskd.pop("template")

        if sub_op == "validate":
            with ctx.loader.validate_unit_models():
                try:
                    UnitModel(**taskd)
                except TypeError as e:
                    logger.error(f"Invalid task definition in {args['schema']}: {e}")
                    return 1
                return None

        logger.info(f"Running task {args['name']}")
        unit_model = UnitModel(**taskd)

        # always raise exceptions
        unit_model.override = (unit_model.override or {}) | {
            "raise_exc": not args["handle_exc"]
        }

        if args["workflow"]:
            process_name = f"{args['workflow']}.{args['name']}"
        else:
            process_name = args["name"]

        if args["resources_in"]:
            with open(args["resources_in"]) as f:
                resources = {k: Resource(**v) for k, v in json.load(f).items()}
        else:
            resources = None

        proc = ctx.loader.new_context_unit(
            model=unit_model,
            context=ctx,
            process_name=process_name,
            sub_type=AbsEventUnit,
            base_log_flag=LogFlag.MANUAL_RUN,
            log_profile=args["log_profile"],
        )
        event_hdlr = EventHandler(
            proc.process_name,
            ctx.get_database(),
            job_sig=proc.job_sig,
            raise_exc=proc.raise_exc,
            resources=resources,
        )
        with self.push_metrics(process_name, logger):
            x = proc.run(event_hdlr=event_hdlr)
        logger.info(f"{unit_model.unit} return value: {x}")

        if args["resources_out"]:
            with open(args["resources_out"], "w") as f:
                json.dump(
                    {k: v.dict() for k, v in (event_hdlr.resources or {}).items()}, f
                )

        dm = divmod(time() - start_time, 60)
        logger.info(
            f"Task processing done. Time required: {round(dm[0])} minutes {round(dm[1])} seconds\n"
        )
        logger.flush()

        return None
