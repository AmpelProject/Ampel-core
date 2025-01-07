import io
import os
import subprocess
import tempfile
from contextlib import nullcontext
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError
from pytest_mock import MockerFixture

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.base.BadConfig import BadConfig
from ampel.config.builder.ConfigChecker import ConfigChecker
from ampel.config.builder.ConfigValidator import ConfigValidator
from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.core.UnitLoader import UnitLoader
from ampel.test.test_JobCommand import run
from ampel.util.mappings import set_by_path


def test_build_config():
    tmp_file = os.path.join(tempfile.mkdtemp(), "ampel_conf.yml")
    io.BytesIO(
        subprocess.check_output(
            [
                "ampel",
                "config",
                "build",
                "--distributions",
                "ampel-interface",
                "ampel-core",
                "-out",
                tmp_file,
            ]
        )
    )
    with open(tmp_file) as f:
        config = yaml.safe_load(f)
    assert ConfigValidator(config).validate() == config


def test_ConfigChecker(testing_config, monkeypatch):
    """
    ConfigValidator validates units without calling their __init__ methods
    """
    with open(testing_config) as f:
        config = yaml.safe_load(f)
    # validates as-is
    checker = ConfigChecker(config)
    checker.validate(raise_exc=True)

    # add a processor with side-effects
    class SideEffect(RuntimeError): ...

    class SideEffectLadenProcessor(AbsEventUnit):
        required: int  # type: ignore[annotation-unchecked]

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            raise SideEffect

        def proceed(self, event_hdlr): ...

    config["process"]["t0"]["BadProcess"] = {
        "name": "BadProcess",
        "version": 0,
        "active": True,
        "schedule": "super",
        "processor": {"unit": "SideEffectLadenProcessor", "config": {"required": 1}},
    }
    checker = ConfigChecker(config)

    def get_class_by_name(name, *args, **kwargs):
        if name == "SideEffectLadenProcessor":
            return SideEffectLadenProcessor
        return UnitLoader.get_class_by_name(checker.loader, name, *args, **kwargs)

    monkeypatch.setattr(checker.loader, "get_class_by_name", get_class_by_name)

    # ConfigChecker attempts to instantiate the unit
    with pytest.raises(SideEffect):
        assert checker.validate(raise_exc=True)

    # ConfigValidator just validates the model
    checker = ConfigValidator(config)
    monkeypatch.setattr(checker.loader, "get_class_by_name", get_class_by_name)
    checker.validate(raise_exc=True)

    # ConfigValidator fails if the config does not satisfy the model
    checker.config["process"]["t0"]["BadProcess"]["processor"]["config"].clear()
    with pytest.raises(BadConfig):
        checker.validate()


@pytest.mark.parametrize("doc", [{"bignumber": 1 << 57}, {1: 2}])
def test_transform_config(doc, tmpdir):
    """Transform preserves objects that are not representable in JSON"""
    infile = Path(tmpdir / "in.yaml")
    outfile = Path(tmpdir / "out.yaml")
    with infile.open("w") as f:
        yaml.dump(doc, f)
    assert (
        run(
            [
                "ampel",
                "config",
                "transform",
                "--file",
                str(infile),
                "--out",
                str(outfile),
                "--filter",
                ".",
            ]
        )
        is None
    )
    with outfile.open() as f:
        transformed_doc = yaml.safe_load(f)
    assert transformed_doc == doc


@pytest.mark.parametrize(
    ("patch", "result"),
    [({}, None), ({"channel.LONG_CHANNEL.purge": {}}, ValidationError)],
)
def test_validate_config(testing_config, tmpdir, patch, result):
    """Validate validates config"""
    tmp_config = tmpdir / "patched_config.yml"
    with testing_config.open() as f:
        config = yaml.safe_load(f)
    for path, item in patch.items():
        set_by_path(config, path, item)
    tmp_config.write_text(yaml.dump(config), "utf-8")
    with pytest.raises(result) if result else nullcontext():
        assert (
            run(
                [
                    "ampel",
                    "config",
                    "validate",
                    "--file",
                    str(tmp_config),
                ]
            )
            == result
        )


def test_collect_bad_unit(tmp_path: Path, mocker: MockerFixture) -> None:
    cb = DistConfigBuilder(DisplayOptions(verbose=True, debug=True))

    bad_unit_config = tmp_path / "unit.yaml"
    with bad_unit_config.open("w") as f:
        yaml.dump(["flerhherher.Floop"], f)

    mocker.patch(
        "ampel.config.builder.DistConfigBuilder.get_files",
        return_value=[bad_unit_config],
    )

    cb.load_distributions()
    assert cb.first_pass_config.has_nested_error()

    with pytest.raises(
        ValueError,
        match=
            r".*Error were reported while gathering configurations \(first pass config\).*",
    ):
        cb.build_config(stop_on_errors=2)
