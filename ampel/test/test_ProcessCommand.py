import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from typing import Any
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
            return main() or None
    except SystemExit as se:
        return se.code


def dump(payload, tmpdir, name: str) -> Path:
    f = Path(tmpdir) / name
    with f.open("w") as fd:
        yaml.dump(payload, fd)
    return f


@pytest.fixture
def vault(tmpdir, secrets):
    return dump(secrets, tmpdir, "secrets.yml")


@pytest.fixture
def secrets():
    return {
        "str": "str",
        "key.with.bunch.of.dots": "nesty",
        "dict": {"user": "none", "password": 1.5},
        "list": [1, 2, 3.5, "flerp"],
        "int": 42,
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


@pytest.fixture
def process_args(testing_config, vault):
    return [
        "--config",
        str(testing_config),
        "--secrets",
        str(vault),
        "--db",
        "whatevs",
        "--log-profile",
        "console_debug",
        "--name",
        "task_1",
    ]


def ingest_config(unit_config: dict[str, Any]):
    return {
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
                                                    "config": unit_config,
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
    }


def test_run_template(
    mock_db: MagicMock,
    process_args: list[str],
    tmpdir,
):
    """ProcessCommand resolves templates"""
    task = dump(
        ingest_config({"foo": 37}),
        tmpdir,
        "task.yml",
    )

    assert run(["ampel", "process", "--schema", str(task), *process_args]) is None, (
        "process runs cleanly"
    )

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
    assert subconfig == {"foo": 37, "secret": None}, "config was hashed recursively"


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"foo": 37, "secret": {"label": "int"}}, None),
        ({"unknown": 37}, 1),
        ({"secret": {"label": "unknown"}}, 1),
        ({"secret": {"label": "str"}}, 1),
    ],
    ids=["valid", "unknown-field", "wrong-secret-label", "wrong-secret-type"],
)
def test_validate_with_templates(
    config: dict[str, Any],
    expected: bool,
    process_args: list[str],
    tmpdir,
):
    """ProcessCommand validates templates"""

    assert (
        run(
            [
                "ampel",
                "process",
                "validate",
                "--schema",
                str(
                    dump(
                        ingest_config(config),
                        tmpdir,
                        "task.yml",
                    )
                ),
                *process_args,
            ]
        )
        == expected
    )
