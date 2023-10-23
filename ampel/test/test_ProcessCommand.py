import base64, json, sys, pytest, yaml
from ampel.t3.T3PlainUnitExecutor import T3PlainUnitExecutor
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock
from pytest_mock import MockerFixture
from contextlib import contextmanager, nullcontext

from ampel.cli.main import main
from ampel.cli.utils import get_vault
from ampel.model.job.JobModel import JobModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.secret.AmpelVault import AmpelVault


@contextmanager
def argv_context(args: list[str]):
    argv = sys.argv
    try:
        sys.argv = args
        yield
    finally:
        sys.argv = argv


def run(args: list[str]) -> None | int | str:
    try:
        with argv_context(args):
            main()
        return None
    except SystemExit as se:
        return se.code


def dump(payload, tmpdir, name) -> Path:
    f = Path(tmpdir) / name
    yaml.dump(payload, f.open("w"))
    return f


@pytest.fixture
def vault(tmpdir):
    return dump({"foo": "bar"}, tmpdir, "secrets.yml")


@pytest.fixture
def secrets():
    return {
        "str": "str",
        "key.with.bunch.of.dots": "nesty",
        "dict": {"user": "none", "password": 1.5},
        "list": [1, 2, 3.5, "flerp"],
    }


@pytest.fixture
def dir_secret_store(tmpdir, secrets):
    parent_dir = Path(tmpdir) / "secrets"
    parent_dir.mkdir()
    for k, v in secrets.items():
        dump(v, parent_dir, k)
    return parent_dir


@pytest.fixture
def schema(tmpdir):
    return dump({"name": "job", "task": [{"unit": "Nonesuch"}]}, tmpdir, "schema.yml")


@pytest.fixture
def mock_db(mocker: MockerFixture):
    return mocker.patch("ampel.core.AmpelDB.AmpelDB.get_collection")


@pytest.fixture
def mock_new_context_unit(mocker: MockerFixture, mock_db):
    return mocker.patch("ampel.core.UnitLoader.UnitLoader.new_context_unit")


def test_resource_passing(
    testing_config,
    mock_db: MagicMock,
    vault: Path,
    tmpdir,
):

    key = "dana"
    value = "zuul"

    writer = dump(
        {
            "unit": "T3Processor",
            "config": {
                "execute": [
                    {
                        "unit": "T3PlainUnitExecutor",
                        "config": {
                            "target": {
                                "unit": "DummyResourceT3Unit",
                                "config": {
                                    "name": key,
                                    "value": value,
                                },
                            }
                        },
                    }
                ]
            },
        },
        tmpdir,
        "writer.yml",
    )

    reader = dump(
        {
            "unit": "DummyResourceInputUnit",
            "config": {"value": key, "expected_value": value},
        },
        tmpdir,
        "reader.yml",
    )

    def run_task(task_path: Path, first: bool = False):
        assert (
            run(
                [
                    "ampel",
                    "process",
                    "--config",
                    str(testing_config),
                    "--secrets",
                    str(vault),
                    "--db",
                    "whatevs",
                    "--log-profile",
                    "console_debug",
                    "--schema",
                    str(task_path),
                    "--resources-in",
                    str(tmpdir / "resources.json") if not first else "",
                    "--resources-out",
                    str(tmpdir / "resources.json"),
                    "--name",
                    "task_1",
                ]
            )
            is None
        )

    run_task(writer, first=True)
    run_task(reader)
