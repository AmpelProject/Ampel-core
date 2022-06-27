import base64
import sys
from contextlib import contextmanager
from os import path
from pathlib import Path
from typing import Optional

import pytest
import yaml
from pytest_mock import MockerFixture

from ampel.cli.JobCommand import JobCommand
from ampel.cli.main import main
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


def run(args: list[str]) -> Optional[int]:
    try:
        with argv_context(args):
            main()
        return None
    except SystemExit as exit:
        return exit.code


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
    loader_vault: Optional[AmpelVault] = new_context_unit.call_args.kwargs[
        "context"
    ].loader.vault
    assert loader_vault is not None
    secret = loader_vault.get_named_secret("foo")
    assert secret is not None
    assert secret.get() == "bar"


def test_resources_from_env(
    testing_config,
    vault: Path,
    schema: Path,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    new_context_unit = mocker.patch("ampel.core.UnitLoader.UnitLoader.new_context_unit")
    monkeypatch.setenv("AMPEL_CONFIG_resource.mongo", "flerp")
    monkeypatch.setenv("AMPEL_CONFIG_resource.herp", "37")
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
    config: AmpelConfig = new_context_unit.call_args.kwargs["context"].config
    assert config.get("resource.mongo") == "flerp"
    assert config.get("resource.herp") == 37


def test_dir_secrets(dir_secret_store, secrets):
    cli_op = JobCommand()
    vault = cli_op.get_vault({"secrets": dir_secret_store})
    for k, v in secrets.items():
        assert vault.get_named_secret(k).get() == v

    assert vault.get_named_secret("nonesuch") is None, "missing secrets skipped cleanly"


def test_parameter_interpolation(
    testing_config,
    vault: Path,
    tmpdir,
):

    value = "flerpyherp"

    path = tmpdir / "token"

    schema = dump(
        {
            "name": "job",
            "parameters": [{"name": "expected_value", "value": value}],
            "task": [
                {
                    "unit": "DummyOutputUnit",
                    "config": {"value": value, "path": str(path)},
                    "outputs": {
                        "parameters": [
                            {"name": "token", "value_from": {"path": str(path)}}
                        ]
                    },
                },
                {
                    "unit": "DummyInputUnit",
                    "config": {
                        "value": "{{ task.DummyOutputUnit.outputs.parameters.token }}",
                        "expected_value": "{{ job.parameters.expected_value }}",
                    },
                },
            ],
        },
        tmpdir,
        "schema.yml",
    )

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


def test_expand_with(
    testing_config,
    vault: Path,
    tmpdir,
):

    values = ["1", "2", "3"]
    paths = [tmpdir / f"token{i}" for i in values]

    schema = dump(
        {
            "name": "job",
            "task": [
                {
                    "unit": "DummyOutputUnit",
                    "config": {"value": "{{ item.value }}", "path": "{{ item.path }}"},
                    "expand_with": {
                        "items": [
                            {"value": v, "path": str(p)} for v, p in zip(values, paths)
                        ]
                    },
                }
            ],
        },
        tmpdir,
        "schema.yml",
    )

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


def test_input_artifacts(
    testing_config,
    vault: Path,
    tmpdir,
):

    value = "flerpyherb"

    path = tmpdir / "token"

    schema = dump(
        {
            "name": "job",
            "parameters": [
                {
                    "name": "expected_value",
                    "value": value,
                },
                {
                    "name": "url",
                    "value": f"https://httpbin.org/base64/{base64.b64encode(value.encode()).decode()}",
                },
            ],
            "task": [
                {
                    "unit": "DummyInputUnit",
                    "config": {
                        "value": "{{ inputs.parameters.token }}",
                        "expected_value": "{{ job.parameters.expected_value }}",
                    },
                    "inputs": {
                        "parameters": [
                            {"name": "token", "value": "{{ inputs.artifacts.token }}"}
                        ],
                        "artifacts": [
                            {
                                "name": "token",
                                "path": str(path),
                                "http": {"url": "{{ job.parameters.url }}"},
                            }
                        ],
                    },
                },
            ],
        },
        tmpdir,
        "schema.yml",
    )

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
