
from argparse import ArgumentParser
from time import time
from typing import Any, Optional, Sequence, Union

import yaml

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from ampel.util.freeze import recursive_freeze


class ProcessCommand(AbsCoreCommand):
    """
    Runs a single process from a task definition (YAML file)

    A process is a single step of a Job (see JobCommand), with all templates resolved.
    """

    def __init__(self):
        self.parser = None

    # Mandatory implementation
    def get_parser(
        self, sub_op: Optional[str] = None
    ) -> Union[ArgumentParser, AmpelArgumentParser]:

        if self.parser:
            return self.parser

        parser = AmpelArgumentParser("process")
        parser.set_help_descr(
            {
                "debug": "Debug",
                # "verbose": "increases verbosity",
                "config": "path to an ampel config file (yaml/json)",
                "schema": "path to YAML job file",
                "name": "process name",
                "secrets": "path to a secret store; either SOPS YAML or mounted k8s Secret directory",
                "db": "database to use",
                "channel": "path to YAML channel file",
                "alias": "path to YAML alias file",
            }
        )

        # Required
        parser.add_arg("config", "required", type=str)
        parser.add_arg("schema", "required")
        parser.add_arg("name", "required")
        parser.add_arg("db", "required", type=str)
        parser.add_arg("channel")
        parser.add_arg("alias")

        # Optional
        parser.add_arg("secrets", type=str)

        # Example
        parser.add_example("process -config ampel_conf.yaml schema task_file.yaml -db processing -name taskytask")
        return parser

    def _get_context(self,
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

        config_dict = ctx.config._config

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
        
        ctx.config._config = recursive_freeze(config_dict)
        
        return ctx

    # Mandatory implementation
    def run(
        self,
        args: dict[str, Any],
        unknown_args: Sequence[str],
        sub_op: Optional[str] = None,
    ) -> None:

        start_time = time()
        logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

        logger.info(f"Running task {args['name']}")

        with open(args["schema"], "r") as f:
            unit_model = UnitModel(**yaml.safe_load(f))
        # always raise exceptions
        unit_model.override = (unit_model.override or {}) | {"raise_exc": True}

        ctx = self._get_context(
            args,
            unknown_args,
            logger,
        )

        proc = ctx.loader.new_context_unit(
            model=unit_model,
            context=ctx,
            process_name=args["name"],
            sub_type=AbsEventUnit,
            base_log_flag=LogFlag.MANUAL_RUN,
        )
        x = proc.run()
        logger.info(f"{unit_model.unit} return value: {x}")

        dm = divmod(time() - start_time, 60)
        logger.info(
            "Task processing done. Time required: %s minutes %s seconds\n"
            % (round(dm[0]), round(dm[1]))
        )
