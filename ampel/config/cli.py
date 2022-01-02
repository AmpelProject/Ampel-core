#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/cli.py
# License:             BSD-3-Clause
# Author:              Jakob van Santen <jakob.van.santen@desy.de>
# Date:                26.08.2020
# Last Modified Date:  26.08.2020
# Last Modified By:    Jakob van Santen <jakob.van.santen@desy.de>

import json, subprocess, sys, yaml
from io import StringIO
from typing import Any, TextIO
from collections.abc import Mapping, Iterable
from argparse import ArgumentParser, ArgumentTypeError, FileType, Namespace

from ampel.base.BadConfig import BadConfig
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.log.utils import log_exception
from ampel.core.AmpelContext import AmpelContext
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.secret.PotemkinSecretProvider import PotemkinSecretProvider


def transform(args: Namespace) -> None:
    """Transform existing configuration with jq"""
    try:
        with FileType()(args.filter) as f:
            jq_args = [f.read()]
    except ArgumentTypeError:
        jq_args = [args.filter]
    # Use a custom transformation to losslessly round-trip from YAML to JSON,
    # in particular:
    # - wrap large ints to prevent truncation to double precision
    # - preserve non-string keys
    input_json = json.dumps(_to_json(yaml.safe_load(args.config_file)))
    config = json.loads(
        subprocess.check_output(["jq"] + jq_args, input=input_json.encode()),
        object_hook=_from_json,
    )
    with StringIO() as output_yaml:
        yaml.dump(config, output_yaml, sort_keys=False)
        if args.validate:
            output_yaml.seek(0)
            _validate(output_yaml)
        output_yaml.seek(0)
        args.output_file.write(output_yaml.read())


def build(args: Namespace) -> int:
    """Build config file from installed distributions"""
    cb = DistConfigBuilder(verbose=args.verbose, get_env=args.get_env)
    try:
        cb.load_distributions()
        config = cb.build_config(
            stop_on_errors=0 if args.ignore_errors else 2,
            config_validator="ConfigValidator",
        )
    except Exception as exc:
        # assume that BadConfig means the error was already logged
        if not isinstance(exc, BadConfig):
            log_exception(cb.logger, exc)
        return 1
    yaml.dump(
        config, args.output_file if args.output_file else sys.stdout, sort_keys=False
    )
    return 0


def _load_dict(source: TextIO) -> dict[str, Any]:
    if isinstance((payload := yaml.safe_load(source)), dict):
        return payload
    else:
        raise TypeError("buf does not deserialize to a dict")


def _validate(config_file: TextIO, secrets: None | TextIO = None) -> None:
    from ampel.model.ChannelModel import ChannelModel
    from ampel.model.ProcessModel import ProcessModel

    ctx = AmpelContext.load(
        _load_dict(config_file),
        secrets=AmpelVault(providers=[(
            DictSecretProvider(_load_dict(secrets))
            if secrets is not None
            else PotemkinSecretProvider()
        )]),
    )
    with ctx.loader.validate_unit_models():
        for channel in ctx.config.get(
            "channel", dict[str, Any], raise_exc=True
        ).values():
            ChannelModel(**{k: v for k, v in channel.items() if k not in {"template"}})
        for tier in range(3):
            for process in ctx.config.get(
                f"process.t{tier}", dict[str, Any], raise_exc=True
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
    p.add_argument("--no-get-env", dest="get_env", default=True, action="store_false",
        help="Skip Python dependency detection when gathering units."
    )
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
    sys.exit(args.func(args))


def _to_json(obj):
    """Get JSON-compliant representation of obj"""
    if isinstance(obj, Mapping):
        assert "__nonstring_keys" not in obj
        doc = {str(k): _to_json(v) for k, v in obj.items()}
        nonstring_keys = {
            str(k): _to_json(k) for k in obj.keys() if not isinstance(k, str)
        }
        if nonstring_keys:
            doc["__nonstring_keys"] = nonstring_keys
        return doc
    elif isinstance(obj, Iterable) and not isinstance(obj, str):
        return [_to_json(v) for v in obj]
    elif isinstance(obj, int) and abs(obj) >> 53:
        # use canonical BSON representation for ints larger than the precision
        # of a double
        return {"$numberLong": str(obj)}
    else:
        return obj


def _from_json(doc):
    """Invert _to_json()"""
    if "$numberLong" in doc:
        return int(doc["$numberLong"])
    elif "__nonstring_keys" in doc:
        nonstring_keys = doc.pop("__nonstring_keys")
        return {nonstring_keys[k]: v for k, v in doc.items()}
    else:
        return doc
