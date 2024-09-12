
import pytest

from ampel.abstract.AbsOpsUnit import AbsOpsUnit
from ampel.base.LogicalUnit import LogicalUnit
from ampel.core.ContextUnit import ContextUnit
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.model.ingest.CompilerOptions import CompilerOptions
from ampel.model.UnitModel import UnitModel
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.secret.NamedSecret import NamedSecret
from ampel.secret.Secret import Secret


@pytest.fixture
def secrets():
    return DictSecretProvider(
        {
            "dict": {"a": 1, "b": "foo"},
            "str": "blahblablah",
            "tuple": (1, 1),
        }
    )

@pytest.fixture
def secret_context(dev_context: DevAmpelContext, secrets, monkeypatch):
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))
    return dev_context


@pytest.mark.parametrize(
    ("expected_type","key"), [(dict, "dict"), (str, "str"), (tuple, "tuple")]
)
def test_resolve_secrets_correct_type(
    dev_context: DevAmpelContext,
    secrets: DictSecretProvider,
    monkeypatch,
    expected_type,
    key,
    ampel_logger,
):
    @dev_context.register_unit
    class Modelo(LogicalUnit):
        seekrit: NamedSecret[expected_type] = NamedSecret[expected_type](label=key)

    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))

    unit = dev_context.loader.new(
        UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
    )
    assert unit.seekrit.get() == secrets.store[key]


@pytest.mark.parametrize(
    ("expected_type","key"), [(dict, "str"), (str, "tuple"), (tuple, "dict")]
)
def test_resolve_secrets_wrong_type(
    secrets, dev_context: DevAmpelContext, monkeypatch, expected_type, key, ampel_logger
):
    @dev_context.register_unit
    class Modelo(LogicalUnit):
        seekrit: NamedSecret[expected_type] = NamedSecret[expected_type](label=key)
    
    vault = AmpelVault(providers=[secrets])
    s = Modelo(logger=ampel_logger).seekrit
    with pytest.raises(ValueError, match="Secret not yet resolved"):
        s.get()
    assert vault.resolve_secret(s, expected_type) is False
    
    monkeypatch.setattr(dev_context.loader, "vault", vault)
    with pytest.raises(TypeError, match=f"Could not resolve Modelo.seekrit as {expected_type.__name__:s}"):
        dev_context.loader.new(
            UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
        )

def test_resolve_secret_in_union(secret_context: DevAmpelContext, ampel_logger):
    """UnitLoader can resolve secret in a union field"""

    @secret_context.register_unit
    class Modelo(LogicalUnit):
        maybe_secret: None | NamedSecret[str] = NamedSecret[str](label="nonesuch")

    unit = secret_context.loader.new(
        UnitModel(unit="Modelo", config={"maybe_secret": {"label": "str"}}), logger=ampel_logger, unit_type=Modelo
    )
    assert unit.maybe_secret is not None
    assert unit.maybe_secret.get() is not None

    with pytest.raises(TypeError, match="Could not resolve Modelo.maybe_secret as str using default value"):
        secret_context.loader.new(
            UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
        )

    unit = secret_context.loader.new(
        UnitModel(unit="Modelo", config={"maybe_secret": None}), logger=ampel_logger, unit_type=Modelo
    )
    assert unit.maybe_secret is None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_resolve_secret_untyped_default(secret_context: DevAmpelContext, ampel_logger):
    """UnitLoader can resolve secret where default is missing a type parameter"""

    @secret_context.register_unit
    class Modelo(LogicalUnit):
        maybe_secret: NamedSecret[str] = NamedSecret(label="str")

    unit = secret_context.loader.new(
        UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
    )
    assert unit.maybe_secret is not None
    assert unit.maybe_secret.get() is not None


def test_resolve_secret_from_superclass(
    secrets: DictSecretProvider, dev_context: DevAmpelContext, monkeypatch, ampel_logger
):
    """Secrets are resolved with multiple inheritance"""

    class Base(ContextUnit):
        seekrit: NamedSecret[dict]

    @dev_context.register_unit
    class Derived(AbsOpsUnit, Base):
        def run(self, beacon):
            ...

    assert (
        "seekrit" not in Derived.__annotations__
    ), "multiply-inherited AmpelBaseModels are missing annotations"

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

    @dev_context.register_unit
    class NiceAndConcrete(LogicalUnit):
        seekrit: NamedSecret[dict]

    # unit with concrete secret field can be instantiated
    dev_context.loader.new(
        UnitModel(unit="NiceAndConcrete", config={"seekrit": {"label": "dict"}}),
        logger=ampel_logger,
        unit_type=NiceAndConcrete,
    )

    # and also validated without instantiating
    with dev_context.loader.validate_unit_models():
        UnitModel(unit="NiceAndConcrete", config={"seekrit": {"label": "dict"}})
        with pytest.raises(TypeError):
            UnitModel(unit="NiceAndConcrete")

    # unit with abstract secret field cannot be instantiated
    @dev_context.register_unit
    class BadAndAbstract(LogicalUnit):
        seekrit: Secret[dict]

    with pytest.raises(TypeError):
        dev_context.loader.new(
            UnitModel(unit="BadAndAbstract", config={"seekrit": {"label": "dict"}}),
            logger=ampel_logger,
            unit_type=BadAndAbstract,
        )


@pytest.mark.parametrize("config", [None, {"seekrit": {"label": "dict"}}])
def test_use_secret_in_init(
    secrets: DictSecretProvider,
    dev_context: DevAmpelContext,
    monkeypatch,
    ampel_logger,
    config,
):
    """Secrets are populated before being passed to init"""
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))

    @dev_context.register_unit
    class NeedsSecretInInit(LogicalUnit):
        seekrit: NamedSecret[dict] = NamedSecret[dict](label="dict")

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            assert (
                self.seekrit.get() == secrets.store[self.seekrit.label]
            ), "secret is populated"
    
    with pytest.raises(ValueError, match="Secret not yet resolved"):
        NeedsSecretInInit(logger=ampel_logger).seekrit.get()

    # secret field is populated
    unit = dev_context.loader.new(
        UnitModel(unit="NeedsSecretInInit", config=config),
        logger=ampel_logger,
        unit_type=NeedsSecretInInit,
    )
    assert unit.seekrit.get() == secrets.store["dict"]


def test_unit_validation(dev_context: DevAmpelContext):
    @dev_context.register_unit
    class Dummy(LogicalUnit):
        param: int = 42

    with dev_context.loader.validate_unit_models():
        # simple, one-level validation
        UnitModel(unit="Dummy")
        UnitModel(unit="Dummy", config={"param": 37})
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy", config={"param": "fish"})
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy", config={"nonexistant_param": True})

        t3_config = dict(
            supply = {
                "unit": "T3DefaultBufferSupplier",
                "config": {
                    "select": {"unit": "T3StockSelector"},
                    "load": {
                        "unit": "T3SimpleDataLoader",
                        "config": {"directives": [{"col": "stock"}]},
                    },
                },
            },
            stage = {
                "unit": "T3SimpleStager",
                "config": {"execute": [{"unit": "DemoT3Unit"}]},
            }
        )

        # recursive validation
        UnitModel(unit="T3Processor", config=t3_config)

        t3_config["supply"]["config"]["select"]["unit"] = "NotActuallyAUnit" # type: ignore[index]
        with pytest.raises(TypeError, match=".*Ampel unit not found: NotActuallyAUnit.*"):
            UnitModel(unit="T3Processor", config=t3_config)


def test_compiler_options_validation(mock_context: DevAmpelContext):
    """AuxAliasableUnit can be intialized from a string"""

    @mock_context.register_unit
    class Dummy(LogicalUnit):
        compiler_options: None | CompilerOptions

    with mock_context.loader.validate_unit_models():
        UnitModel(unit="Dummy")
        UnitModel(unit="Dummy", config={"compiler_options": "DummyCompilerOptions"})
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy", config={"compiler_options": "foo"})


def test_secret_validation(secret_context: DevAmpelContext):
    @secret_context.register_unit
    class Dummy(LogicalUnit):
        seekrit: NamedSecret[str] = NamedSecret[str](label="foo")

    with secret_context.loader.validate_unit_models():
        # secret does not exist
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy")
        # exists, but wrong type
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy", config={"seekrit": {"label": "dict"}})
        # exists with correct type
        UnitModel(unit="Dummy", config={"seekrit": {"label": "str"}})


def test_result_adapter_trace(mock_context: DevAmpelContext):
    from ampel.test.dummy import DummyUnitResultAdapter

    mock_context.register_unit(DummyUnitResultAdapter)
    model = UnitModel(unit="DummyUnitResultAdapter")
    u1 = mock_context.loader.new_context_unit(model, mock_context, run_id=1)
    u2 = mock_context.loader.new_context_unit(model, mock_context, run_id=2)
    assert (
        u1._get_trace_content() == u2._get_trace_content()  # noqa: SLF001
    ), "trace content is identical for different run_id"
    assert u1._trace_id == u2._trace_id, "trace id is identical for different run_id"  # noqa: SLF001
