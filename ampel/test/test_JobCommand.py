import sys
from contextlib import contextmanager
from os import path
from pathlib import Path
from typing import Optional

import pytest
import yaml
from pytest_mock import MockerFixture

from ampel.cli.main import main
from ampel.secret.AmpelVault import AmpelVault


@contextmanager
def argv_context(args: list[str]):
    argv = sys.argv
    try:
        sys.argv = args
        yield
    finally:
        sys.argv = argv


def run(args: list[str]) -> Optional[int]:
    try:
        with argv_context(args):
            return main()
    except SystemExit as exit:
        print("exitus")
        return exit.code


def dump(payload, tmpdir, name):
    f = Path(tmpdir) / name
    yaml.dump(payload, f.open("w"))
    return f


@pytest.fixture
def vault(tmpdir):
    return dump({"foo": "bar"}, tmpdir, "secrets.yml")


@pytest.fixture
def schema(tmpdir):
    return dump({"name": "job", "task": [{"unit": "Nonesuch"}]}, tmpdir, "schema.yml")


def test_secrets(testing_config, vault: Path, schema: Path, mocker: MockerFixture):
    new_context_unit = mocker.patch("ampel.core.UnitLoader.UnitLoader.new_context_unit")
    assert (
        run(
            [
                "ampel",
                "job",
                "--config",
                str(testing_config),
                "--secrets",
                str(vault),
                "--schema",
                str(schema),
            ]
        )
        == None
    )
    assert new_context_unit.call_count == 1
    vault: Optional[AmpelVault] = new_context_unit.call_args.kwargs[
        "context"
    ].loader.vault
    assert vault is not None
    secret = vault.get_named_secret("foo")
    assert secret.get() == "bar"
