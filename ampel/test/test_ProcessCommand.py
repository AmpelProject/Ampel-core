import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pytest_mock import MockerFixture

from ampel.cli.main import main


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


def dump(payload, tmpdir, name: str) -> Path:
    f = Path(tmpdir) / name
    with f.open("w") as fd:
        yaml.dump(payload, fd)
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
            "unit": "DummyResourceOutputUnit",
            "config": {
                "name": key,
                "value": value,
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


def test_templates(
    testing_config,
    mock_db: MagicMock,
    vault: Path,
    tmpdir,
):
    """ProcessCommand resolves templates"""
    task = dump(
        {
            "template": "hash_t2_config",
            "unit": "DummyIngestUnit",
            "config": dict(
                directives=[
                    dict(
                        channel="TEST",
                        ingest=dict(
                            combine=[
                                dict(
                                    unit="T1SimpleCombiner",
                                    state_t2=[
                                        dict(
                                            unit="DummyTiedStateT2Unit",
                                            config={
                                                "t2_dependency": [
                                                    {
                                                        "unit": "DummyStateT2Unit",
                                                        "config": {"foo": 37},
                                                    }
                                                ]
                                            },
                                        )
                                    ],
                                )
                            ]
                        ),
                    )
                ]
            ),
        },
        tmpdir,
        "task.yml",
    )

    def run_task(task_path: Path):
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
                    "--name",
                    "task_1",
                ]
            )
            is None
        )

    run_task(task)

    conf = mock_db("conf")
    # get the last config inserted
    doc = next(
        d.args[0]
        for d in reversed(conf.insert_one.call_args_list)
        if "unit" in d.args[0]
    )
    assert doc["unit"] == "DummyIngestUnit"
    config_id = doc["config"]["directives"][0]["ingest"]["combine"][0]["state_t2"][0][
        "config"
    ]

    def get_config(config_id: int):
        return next(
            (
                d.args[0]
                for d in reversed(conf.insert_one.call_args_list)
                if d.args[0].get("_id") == config_id
            ),
            None,
        )

    assert isinstance(config_id, int), "config was hashed"
    config = get_config(config_id)
    assert "t2_dependency" in config
    subconfig = get_config(config["t2_dependency"][0]["config"])
    subconfig.pop("_id")
    assert subconfig == {"foo": 37}, "config was hashed recursively"
