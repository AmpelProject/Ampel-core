import pytest

from pydantic import create_model

from ampel.model.StrictModel import StrictModel
from ampel.model.Secret import Secret
from ampel.core.UnitLoader import UnitLoader
from ampel.dev.DictSecretProvider import DictSecretProvider


@pytest.fixture
def secrets():
    return DictSecretProvider(
        {"dict": {"a": 1, "b": "foo"}, "str": "blahblablah", "tuple": (1, 1),}
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
