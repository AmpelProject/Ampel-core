import io, pytest, yaml, subprocess
from argparse import Namespace

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.base.BadConfig import BadConfig
from ampel.config.builder.ConfigChecker import ConfigChecker
from ampel.config.builder.ConfigValidator import ConfigValidator
from ampel.core.UnitLoader import UnitLoader
from ampel.config import cli


def test_build_config():
    config = yaml.safe_load(
        io.BytesIO(subprocess.check_output(["ampel-config", "build"]))
    )
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

        def run(self):
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
def test_transform_config(doc):
    """Transform preserves objects that are not representable in JSON"""
    args = Namespace(
        config_file=io.StringIO(), filter=".", output_file=io.StringIO(), validate=False
    )
    yaml.dump(doc, args.config_file)
    args.config_file.seek(0)
    cli.transform(args)
    args.output_file.seek(0)
    transformed_doc = yaml.safe_load(args.output_file)
    assert transformed_doc == doc
