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
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.core.EventHandler import EventHandler
from ampel.core.Schedulable import Schedulable
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from ampel.struct.Resource import Resource
from ampel.util.freeze import recursive_freeze


def _handle_traceback(signal, frame):
    print(traceback.print_stack(frame))


class ProcessCommand(AbsCoreCommand):
    """
    Runs a single process from a task definition (YAML file)

    A process is a single step of a Job (see JobCommand), with all templates resolved.
    """

    def __init__(self):
        self.parser = None

    @staticmethod
    def get_sub_ops() -> None | list[str]:
        return None

    # Mandatory implementation
    def get_parser(
        self, sub_op: None | str = None
    ) -> ArgumentParser | AmpelArgumentParser:

        if self.parser:
            return self.parser

        parser = AmpelArgumentParser("process")
        parser.set_help_descr(
            {
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
        )

        parser.req("config")
        parser.req("schema")
        parser.req("name")
        parser.req("db", type=str)

        parser.opt("resources-in")
        parser.opt("resources-out")

        parser.opt("channel")
        parser.opt("alias")
        parser.opt("workflow", default=None, type=str)
        parser.opt("log-profile", default="prod")
        parser.opt("debug", default=False, action="store_true")
        parser.opt("handle-exc", default=False, action="store_true")
        parser.opt("secrets", type=str)

        # Example
        parser.example(
            "process -config ampel_conf.yaml schema task_file.yaml -db processing -name taskytask"
        )
        return parser

    def _get_context(
        self,
        args: dict[str, Any],
        unknown_args: Sequence[str],
        logger: AmpelLogger,
    ) -> DevAmpelContext:

        # DevAmpelContext hashes automatically confid from potential IngestDirectives
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
    ) -> None:

        if args["debug"]:
            signal.signal(signal.SIGUSR1, _handle_traceback)

        start_time = time()
        logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

        logger.info(f"Running task {args['name']}")

        with open(args["schema"]) as f:
            unit_model = UnitModel(**yaml.safe_load(f))
        # always raise exceptions
        unit_model.override = (unit_model.override or {}) | {
            "raise_exc": not args["handle_exc"]
        }

        ctx = self._get_context(
            args,
            unknown_args,
            logger,
        )

        if args["workflow"]:
            process_name = f'{args["workflow"]}.{args["name"]}'
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
