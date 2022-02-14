from typing import Any
from ampel.base.LogicalUnit import LogicalUnit
from ampel.secret.NamedSecret import NamedSecret
from ampel.dev.DevAmpelContext import DevAmpelContext
import pytest

from pydantic import create_model, ValidationError

from ampel.model.StrictModel import StrictModel
from ampel.secret.Secret import Secret
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.secret.AmpelVault import AmpelVault
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsOpsUnit import AbsOpsUnit
from ampel.core.ContextUnit import ContextUnit


@pytest.fixture
def secrets():
    return DictSecretProvider(
        {
            "dict": {"a": 1, "b": "foo"},
            "str": "blahblablah",
            "tuple": (1, 1),
        }
    )


def resolve_secrets_args(expected_type, key):
    fields = {"seekrit": (Secret[expected_type], {"key": key})}
    model = create_model("testy", __config__=StrictModel.__config__, **fields)
    return model, model.__annotations__, model.__field_defaults__, {}


@pytest.mark.parametrize(
    "expected_type,key", [(dict, "dict"), (str, "str"), (tuple, "tuple")]
)
def test_resolve_secrets_correct_type(
    dev_context: DevAmpelContext,
    secrets: DictSecretProvider,
    monkeypatch,
    expected_type,
    key,
    ampel_logger,
):
    class Modelo(LogicalUnit):
        seekrit: NamedSecret[expected_type] = NamedSecret(label=key)  # type: ignore[valid-type]

    dev_context.register_unit(Modelo)
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))

    unit = dev_context.loader.new(
        UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
    )
    assert unit.seekrit.get() == secrets.store[key]


@pytest.mark.parametrize(
    "expected_type,key", [(dict, "str"), (str, "tuple"), (tuple, "dict")]
)
def test_resolve_secrets_wrong_type(
    secrets, dev_context: DevAmpelContext, monkeypatch, expected_type, key, ampel_logger
):
    class Modelo(LogicalUnit):
        seekrit: NamedSecret[expected_type] = NamedSecret(label=key)  # type: ignore[valid-type]

    dev_context.register_unit(Modelo)
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))
    with pytest.raises(ValueError):
        dev_context.loader.new(
            UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
        )


def test_resolve_secret_from_superclass(
    secrets: DictSecretProvider, dev_context: DevAmpelContext, monkeypatch, ampel_logger
):
    """Secrets are resolved with multiple inheritance"""
    class Base(ContextUnit):
        seekrit: NamedSecret[dict]

    class Derived(AbsOpsUnit, Base):
        def run(self, beacon):
            ...
    assert "seekrit" not in Derived.__annotations__, "multiply-inherited AmpelBaseModels are missing annotations"

    dev_context.register_unit(Derived) # type: ignore[arg-type]
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))
    unit = dev_context.loader.new(
        UnitModel(unit="Derived", config={"seekrit": {"label": "dict"}}),
        logger=ampel_logger,
        context=dev_context,
        unit_type=Derived,
    )
    assert unit.seekrit.get() == secrets.store.get("dict")


def test_resolve_secret_from_config(
    secrets, dev_context: DevAmpelContext, monkeypatch, ampel_logger
):
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))

    class NiceAndConcrete(LogicalUnit):
        seekrit: NamedSecret[dict]

    dev_context.register_unit(NiceAndConcrete)
    # unit with concrete secret field can be instantiated
    dev_context.loader.new(
        UnitModel(unit="NiceAndConcrete", config={"seekrit": {"label": "dict"}}),
        logger=ampel_logger,
        unit_type=NiceAndConcrete,
    )

    # and also validated without instantiating
    with dev_context.loader.validate_unit_models():
        UnitModel(unit="NiceAndConcrete", config={"seekrit": {"label": "dict"}})
        with pytest.raises(ValidationError):
            UnitModel(unit="NiceAndConcrete")

    # unit with abstract secret field cannot be instantiated
    class BadAndAbstract(LogicalUnit):
        seekrit: Secret[dict]

    dev_context.register_unit(BadAndAbstract)
    with pytest.raises(ValidationError):
        dev_context.loader.new(
            UnitModel(unit="BadAndAbstract", config={"seekrit": {"label": "dict"}}),
            logger=ampel_logger,
            unit_type=BadAndAbstract,
        )


def test_validator_patching():
    """
    Model validation can be monkeypatched in a context manager (tripwire for
    changes in pydantic internals)
    """
    from functools import partial
    from contextlib import contextmanager
    from ampel.model.StrictModel import StrictModel

    class Model(StrictModel):
        name: str

    # add extra argument to be bound with partial()
    def validate(cls, values, other_arg):
        assert other_arg == "pass"
        return values

    @contextmanager
    def add_root_validator(model_class, func):
        extra_validator = (False, func)
        model_class.__post_root_validators__.append(extra_validator)
        try:
            yield
        finally:
            model_class.__post_root_validators__.remove(extra_validator)

    # outside context, extra validator is not run
    Model(name="fred")

    # inside context, extra validator runs (and raises an exception)
    with pytest.raises(ValidationError):
        with add_root_validator(Model, partial(validate, other_arg="fail")):
            Model(name="fred")
    # or passes, if configured to do so
    with add_root_validator(Model, partial(validate, other_arg="pass")):
        Model(name="fred")

    # outside context, extra validator is gone again
    Model(name="fred")


def test_unit_validation(dev_context: DevAmpelContext):
    class Dummy(LogicalUnit):
        param: int = 42

    dev_context.register_unit(Dummy)

    with dev_context.loader.validate_unit_models():
        # simple, one-level validation
        UnitModel(unit="Dummy")
        UnitModel(unit="Dummy", config={"param": 37})
        with pytest.raises(ValidationError):
            UnitModel(unit="Dummy", config={"param": "fish"})
        with pytest.raises(ValidationError):
            UnitModel(unit="Dummy", config={"nonexistant_param": True})

        t3_config: dict[str, Any] = {
            "execute": [
                {
                    "unit": "T3ReviewUnitExecutor",
                    "config": {
                        "supply": {
                            "unit": "T3DefaultBufferSupplier",
                            "config": {
                                "select": {"unit": "T3StockSelector"},
                                "load": {
                                    "unit": "T3SimpleDataLoader",
                                    "config": {"directives": [{"col": "stock"}]},
                                },
                            },
                        },
                        "stage": {"unit": "T3SimpleStager", "config": {"execute": []}},
                    },
                }
            ],
        }

        # recursive validation
        UnitModel(unit="T3Processor", config=t3_config)

        with pytest.raises(ValidationError):
            t3_config["execute"][0]["config"]["supply"]["config"]["select"][
                "unit"
            ] = "NotActuallyAUnit"
            UnitModel(unit="T3Processor", config=t3_config)
