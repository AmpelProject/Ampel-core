import base64, json, sys, pytest, yaml
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock
from pytest_mock import MockerFixture
from contextlib import contextmanager

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


@pytest.fixture
def mock_db(mocker: MockerFixture):
    return mocker.patch("ampel.core.AmpelDB.AmpelDB.get_collection")


@pytest.fixture
def mock_new_context_unit(mocker: MockerFixture, mock_db):
    return mocker.patch("ampel.core.UnitLoader.UnitLoader.new_context_unit")


def test_secrets(testing_config, vault: Path, schema: Path, mock_new_context_unit):
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
    assert mock_new_context_unit.call_count == 1
    loader_vault: Optional[AmpelVault] = mock_new_context_unit.call_args.kwargs[
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
    mock_new_context_unit,
    monkeypatch: pytest.MonkeyPatch
):
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
    config: AmpelConfig = mock_new_context_unit.call_args.kwargs["context"].config
    assert config.get("resource.mongo") == "flerp"
    assert config.get("resource.herp") == 37


def test_dir_secrets(dir_secret_store, secrets):
    vault = get_vault({"secrets": dir_secret_store})
    for k, v in secrets.items():
        assert vault.get_named_secret(k).get() == v

    assert vault.get_named_secret("nonesuch") is None, "missing secrets skipped cleanly"


def test_parameter_interpolation(
    testing_config,
    mock_db,
    vault: Path,
    tmpdir
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
    mock_db,
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
    mock_db,
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


def test_template_resolution(
    testing_config,
    mock_db: MagicMock,
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
                    "template": "dummy_processor",
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

    update_one = mock_db().update_one
    inserted_job_def = update_one.call_args_list[0].args[1]["$setOnInsert"]
    parsed_job_def = json.loads(
        JobModel(**yaml.safe_load((tmpdir / "schema.yml").open())).json(exclude_unset=True)
    )
    # fake template resolution
    del parsed_job_def["task"][0]["template"]
    parsed_job_def["task"][0]["unit"] = "DummyInputUnit"

    assert (
        inserted_job_def == parsed_job_def
    ), "templates were resolved in job def inserted into db"
