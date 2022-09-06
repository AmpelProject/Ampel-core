from contextlib import nullcontext
import io, pytest, yaml, subprocess, tempfile, os
from pathlib import Path
from argparse import Namespace

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.base.BadConfig import BadConfig
from ampel.config.builder.ConfigChecker import ConfigChecker
from ampel.config.builder.ConfigValidator import ConfigValidator
from ampel.core.UnitLoader import UnitLoader
from ampel.util.mappings import set_by_path

from ampel.test.test_JobCommand import run


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
    with open(tmp_file, "r") as f:
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
    class SideEffect(RuntimeError):
        ...

    class SideEffectLadenProcessor(AbsEventUnit):

        required: int

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            raise SideEffect

        def proceed(self, event_hdlr):
            ...

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
        else:
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
    yaml.dump(doc, infile.open("w"))
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
        == None
    )
    transformed_doc = yaml.safe_load(outfile.open())
    assert transformed_doc == doc


@pytest.mark.parametrize(
    "patch,result", [({}, None), ({"channel.LONG_CHANNEL.purge": {}}, TypeError)]
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
