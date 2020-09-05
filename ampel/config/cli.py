#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/cli.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 26.08.2020
# Last Modified Date: 26.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import sys
from argparse import (Action, ArgumentParser, ArgumentTypeError, FileType,
                      Namespace, SUPPRESS)
from io import StringIO
from typing import Any, Dict, Optional, TextIO

import yaml
from yq import yq # type: ignore[import]

from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.core import AmpelContext
from ampel.dev.DictSecretProvider import (DictSecretProvider,
                                          PotemkinSecretProvider)


def transform(args: Namespace) -> None:
    """Transform existing configuration with jq"""
    input_streams = [args.config_file]
    try:
        with FileType()(args.filter) as f:
            jq_args = [f.read()]
    except ArgumentTypeError:
        jq_args = [args.filter]
    if args.validate:
        output = StringIO()
    else:
        output = args.output_file
    yq(
        input_streams,
        output_stream=output,
        input_format="yaml",
        output_format="yaml",
        jq_args=jq_args,
        exit_func=lambda code: None
    )
    if args.validate:
        output.seek(0)
        _validate(output)
        output.seek(0)
        args.output_file.write(output.read())


def build(args: Namespace) -> None:
    """Build config file from installed distributions"""
    cb = DistConfigBuilder(verbose=args.verbose)
    cb.load_distributions()

    config = cb.build_config(ignore_errors=args.ignore_errors)
    yaml.dump(
        config, args.output_file if args.output_file else sys.stdout, sort_keys=False
    )

def _load_dict(source: TextIO) -> Dict[str, Any]:
    if isinstance((payload := yaml.safe_load(source)), dict):
        return payload
    else:
        raise TypeError(f"buf does not deserialize to a dict")

def _validate(config_file: TextIO, secrets: Optional[TextIO] = None) -> None:
    from ampel.model.ChannelModel import ChannelModel
    from ampel.model.ProcessModel import ProcessModel
    from ampel.model.UnitModel import UnitModel
    ctx = AmpelContext.new(
        AmpelConfig.new(_load_dict(config_file)),
        secrets=(
            DictSecretProvider(_load_dict(secrets))
            if secrets is not None
            else PotemkinSecretProvider()
        ),
    )
    UnitModel._unit_loader = ctx.loader
    for channel in ctx.config.get(
        "channel",
        Dict[str,Any],
        raise_exc=True
    ).values():
        ChannelModel(**{
            k:v for k,v in channel.items()
            if not k in {"template"}
        })
    for tier in range(3):
        for process in ctx.config.get(
            f"process.t{tier}",
            Dict[str,Any],
            raise_exc=True
        ).values():
            ProcessModel(**process)


def validate(args: Namespace) -> None:
    """Validate configuration"""
    _validate(args.config_file, args.secrets)


def main():
    parser = ArgumentParser()

    subparsers = parser.add_subparsers(help="command help", dest="command")
    subparsers.required = True

    def add_command(f, name=None):
        if name is None:
            name = f.__name__
        p = subparsers.add_parser(name, help=f.__doc__)
        p.set_defaults(func=f)
        return p

    p = add_command(build)
    p.add_argument("-v", "--verbose", default=False, action="store_true")
    p.add_argument("--ignore-errors", default=False, action="store_true")
    p.add_argument("-o", "--output-file", type=FileType("w"))

    p = add_command(validate)
    p.add_argument("config_file", type=FileType("r"))
    p.add_argument("--secrets", type=FileType("r"))

    p = add_command(transform)
    p.add_argument("filter", help="jq filter (or path to file containing one)")
    p.add_argument("config_file", type=FileType("r"))
    p.add_argument(
        "--validate",
        default=False,
        action="store_true",
        help="Validate config after applying transformation",
    )
    p.add_argument("-o", "--output-file", type=FileType("w"), default=sys.stdout)

    args = parser.parse_args()
    args.func(args)
