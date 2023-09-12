import sys, pytest, yaml
from pathlib import Path
from typing import Optional
from pytest_mock import MockerFixture
from contextlib import contextmanager

from ampel.cli.main import main
from ampel.cli.utils import get_vault
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
        ) is None
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
        ) is None
    )
    config: AmpelConfig = mock_new_context_unit.call_args.kwargs["context"].config
    assert config.get("resource.mongo") == "flerp"
    assert config.get("resource.herp") == 37


def test_dir_secrets(dir_secret_store, secrets):
    vault = get_vault({"secrets": dir_secret_store})
    for k, v in secrets.items():
        assert vault.get_named_secret(k).get() == v

    assert vault.get_named_secret("nonesuch") is None, "missing secrets skipped cleanly"
