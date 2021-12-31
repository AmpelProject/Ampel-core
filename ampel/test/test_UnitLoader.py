from typing import Any
from ampel.base.LogicalUnit import LogicalUnit
from ampel.secret.NamedSecret import NamedSecret
from ampel.dev.DevAmpelContext import DevAmpelContext
import pytest

from ampel.secret.Secret import Secret
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.secret.AmpelVault import AmpelVault
from ampel.model.UnitModel import UnitModel


@pytest.fixture
def secrets():
    return DictSecretProvider(
        {
            "dict": {"a": 1, "b": "foo"},
            "str": "blahblablah",
            "tuple": (1, 1),
        }
    )


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


def test_resolve_secret_from_config(secrets, dev_context: DevAmpelContext, monkeypatch, ampel_logger):
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))

    class NiceAndConcrete(LogicalUnit):
        seekrit: NamedSecret[dict]

    dev_context.register_unit(NiceAndConcrete)
    # unit with concrete secret field can be instantiated
    dev_context.loader.new(
        UnitModel(unit="NiceAndConcrete", config={"seekrit": {"label": "dict"}}),
        logger=ampel_logger,
        unit_type=NiceAndConcrete
    )

    # and also validated without instantiating
    with dev_context.loader.validate_unit_models():
        UnitModel(unit="NiceAndConcrete", config={"seekrit": {"label": "dict"}})
        with pytest.raises(ValueError):
            UnitModel(unit="NiceAndConcrete")

    # unit with abstract secret field cannot be instantiated
    class BadAndAbstract(LogicalUnit):
        seekrit: Secret[dict]

    dev_context.register_unit(BadAndAbstract)
    with pytest.raises(TypeError):
        dev_context.loader.new(
            UnitModel(unit="BadAndAbstract", config={"seekrit": {"label": "dict"}}),
            logger=ampel_logger,
            unit_type=BadAndAbstract
        )


def test_unit_validation(dev_context: DevAmpelContext):
    class Dummy(LogicalUnit):
        param: int = 42

    dev_context.register_unit(Dummy)

    with dev_context.loader.validate_unit_models():
        # simple, one-level validation
        UnitModel(unit="Dummy")
        UnitModel(unit="Dummy", config={"param": 37})
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy", config={"param": "fish"})
        with pytest.raises(ValueError):
            UnitModel(unit="Dummy", config={"nonexistant_param": True})

        t3_config: dict[str, Any] = {
            "process_name": "test",
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
                                    "config": {
                                        "directives": [{"col": "stock"}]
                                    }
                                }
                            }
                        },
                        "stage": {
                            "unit": "T3SimpleStager",
                            "config": {"execute": [{"unit": "DemoReviewT3Unit"}]}
                        }
                    }
                }
            ]
        }

        # recursive validation
        UnitModel(unit="T3Processor", config=t3_config)

        #with pytest.raises(ValueError):
        #    t3_config["execute"][0]["config"]["supply"]["config"]["select"]["unit"] = "NotActuallyAUnit"
        #    UnitModel(unit="T3Processor", config=t3_config)
        #t3_config["execute"][0]["config"]["supply"]["config"]["select"]["unit"] = "NotActuallyAUnit"
        UnitModel(unit="T3Processor", config=t3_config)
