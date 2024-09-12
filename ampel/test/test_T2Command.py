import sys
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path

import pytest
import yaml
from mongomock import MongoClient
from pytest_mock import MockerFixture

from ampel.cli.main import main
from ampel.cli.T2Command import T2Command
from ampel.core.AmpelDB import UnknownDatabase


@contextmanager
def argv_context(args: Sequence[str]):
    argv = sys.argv
    try:
        sys.argv = list(args)
        yield
    finally:
        sys.argv = argv


def run(args: Sequence[str]) -> None | int | str:
    try:
        with argv_context(args):
            main()
        return None
    except SystemExit as se:
        return se.code


def dump(payload, tmpdir, name: str) -> Path:
    f = Path(tmpdir) / name
    with f.open("w") as fd:
        yaml.dump(payload, fd)
    return f


@pytest.fixture
def secrets():
    return {
        "mongo/writer": {"username": "writer", "password": "writer"},
        "mongo/logger": {"username": "logger", "password": "logger"},
    }


@pytest.fixture
def vault(tmpdir, secrets):
    return dump(secrets, tmpdir, "secrets.yml")


@pytest.fixture
def check_mongo_auth(mocker):
    def checky(*args, **kwargs):
        assert {"username", "password"}.issubset(kwargs.keys())
        return MongoClient(*args, **kwargs)

    return mocker.patch("ampel.core.AmpelDB.MongoClient", side_effect=checky)


@pytest.fixture
def ampel_cli_opts(testing_config, vault):
    return ["--secrets", str(vault), "--config", str(testing_config)]


def test_auth(ampel_cli_opts, check_mongo_auth):

    cli_op, sub_op = T2Command(), "reset"
    parser = cli_op.get_parser(sub_op)
    args, unknown_args = parser.parse_known_args(ampel_cli_opts)

    with pytest.raises(UnknownDatabase):
        cli_op.run(vars(args), unknown_args, sub_op)
    assert check_mongo_auth.called
    for call_args in check_mongo_auth.call_args_list:
        assert {"username", "password"}.issubset(call_args.kwargs.keys())


def test_multi_integer_flags(ampel_cli_opts, check_mongo_auth, mocker: MockerFixture):
    """
    short-option argument parsing handles negative integers
    """

    mock_run = mocker.patch("ampel.cli.T2Command.T2Command.run")

    run("ampel t2 reset -code -5 -7 -2006".split())

    assert mock_run.called
    args, unknown_args, sub_op = mock_run.call_args[0]
    assert not unknown_args, "no unhandled arguments"
    assert args["code"] == [-5, -7, -2006], "all codes assigned to correct option"
