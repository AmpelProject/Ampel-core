import pytest

from pydantic import create_model, ValidationError

from ampel.model.StrictModel import StrictModel
from ampel.model.Secret import Secret
from ampel.core.UnitLoader import UnitLoader
from ampel.dev.DictSecretProvider import DictSecretProvider
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


def resolve_secrets_args(expected_type, key):
    fields = {"seekrit": (Secret[expected_type], {"key": key})}
    model = create_model("testy", __config__=StrictModel.__config__, **fields)
    return model, model.__annotations__, model.__field_defaults__, {}


@pytest.mark.parametrize(
    "expected_type,key", [(dict, "dict"), (str, "str"), (tuple, "tuple")]
)
def test_resolve_secrets_correct_type(secrets, expected_type, key):
    assert (
        UnitLoader.resolve_secrets(secrets, *resolve_secrets_args(expected_type, key))[
            "seekrit"
        ].get()
        == secrets.store[key]
    )


@pytest.mark.parametrize(
    "expected_type,key", [(dict, "str"), (str, "tuple"), (tuple, "dict")]
)
def test_resolve_secrets_wrong_type(secrets, expected_type, key):
    with pytest.raises(ValueError):
        UnitLoader.resolve_secrets(secrets, *resolve_secrets_args(expected_type, key))


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


def test_unit_validation(dev_context):

    with dev_context.loader.validate_unit_models():
        # simple, one-level validation
        UnitModel(unit="Sleepy")
        with pytest.raises(ValidationError):
            UnitModel(unit="Sleepy", config={"nonexistant_param": True})

        # recursive validation
        UnitModel(
            **{
                "unit": "T3Processor",
                "config": {
                    "directives": [
                        {
                            "load": {
                                "unit": "T3SimpleDataLoader",
                                "config": {"directives": []},
                            },
                            "run": {
                                "unit": "T3UnitRunner",
                                "config": {"directives": []},
                            },
                        }
                    ]
                },
            }
        )

        with pytest.raises(ValidationError):
            UnitModel(
                **{
                    "unit": "T3Processor",
                    "config": {
                        "directives": [
                            {
                                "load": {"unit": "NotActuallyAUnit"},
                                "run": {"unit": "T3UnitRunner"},
                            }
                        ]
                    },
                }
            )
