
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


@pytest.fixture()
def secrets():
    return DictSecretProvider(
        {
            "dict": {"a": 1, "b": "foo"},
            "str": "blahblablah",
            "tuple": (1, 1),
        }
    )

@pytest.fixture()
def secret_context(dev_context: DevAmpelContext, secrets, monkeypatch):
    monkeypatch.setattr(dev_context.loader, "vault", AmpelVault(providers=[secrets]))
    return dev_context


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
        seekrit: NamedSecret[expected_type] = NamedSecret[expected_type](label=key)  # type: ignore[valid-type]

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
        seekrit: NamedSecret[expected_type] = NamedSecret[expected_type](label=key)  # type: ignore[valid-type]

    dev_context.register_unit(Modelo)
    
    vault = AmpelVault(providers=[secrets])
    s = Modelo(logger=ampel_logger).seekrit
    with pytest.raises(ValueError):
        s.get()
    assert vault.resolve_secret(s, expected_type) is False
    
    monkeypatch.setattr(dev_context.loader, "vault", vault)
    with pytest.raises(ValueError):
        dev_context.loader.new(
            UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
        )

def test_resolve_secret_in_union(secret_context: DevAmpelContext, ampel_logger):
    """UnitLoader can resolve secret in a union field"""

    class Modelo(LogicalUnit):
        maybe_secret: None | NamedSecret[str] = NamedSecret[str](label="str")
    secret_context.register_unit(Modelo)

    unit = secret_context.loader.new(
        UnitModel(unit="Modelo"), logger=ampel_logger, unit_type=Modelo
    )
    assert unit.maybe_secret is not None
    assert unit.maybe_secret.get() is not None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_resolve_secret_untyped_default(secret_context: DevAmpelContext, ampel_logger):
    """UnitLoader can resolve secret where default is missing a type parameter"""

    class Modelo(LogicalUnit):
        maybe_secret: NamedSecret[str] = NamedSecret(label="str")
    secret_context.register_unit(Modelo)

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

    class Derived(AbsOpsUnit, Base):
        def run(self, beacon):
            ...

    assert (
        "seekrit" not in Derived.__annotations__
    ), "multiply-inherited AmpelBaseModels are missing annotations"

    dev_context.register_unit(Derived)  # type: ignore[arg-type]
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
        with pytest.raises(TypeError):
            UnitModel(unit="NiceAndConcrete")

    # unit with abstract secret field cannot be instantiated
    class BadAndAbstract(LogicalUnit):
        seekrit: Secret[dict]

    dev_context.register_unit(BadAndAbstract)
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

    class NeedsSecretInInit(LogicalUnit):
        seekrit: NamedSecret[dict] = NamedSecret[dict](label="dict")

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            assert (
                self.seekrit.get() == secrets.store[self.seekrit.label]
            ), "secret is populated"
    
    with pytest.raises(ValueError):
        NeedsSecretInInit(logger=ampel_logger).seekrit.get()

    dev_context.register_unit(NeedsSecretInInit)
    # secret field is populated
    unit = dev_context.loader.new(
        UnitModel(unit="NeedsSecretInInit", config=config),
        logger=ampel_logger,
        unit_type=NeedsSecretInInit,
    )
    assert unit.seekrit.get() == secrets.store["dict"]


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

        with pytest.raises(TypeError):
            t3_config["supply"]["config"]["select"]["unit"] = "NotActuallyAUnit" # type: ignore
            UnitModel(unit="T3Processor", config=t3_config)


def test_compiler_options_validation(mock_context: DevAmpelContext):
    """AuxAliasableUnit can be intialized from a string"""

    class Dummy(LogicalUnit):
        compiler_options: None | CompilerOptions

    mock_context.register_unit(Dummy)

    with mock_context.loader.validate_unit_models():
        UnitModel(unit="Dummy")
        UnitModel(unit="Dummy", config={"compiler_options": "DummyCompilerOptions"})
        with pytest.raises(TypeError):
            UnitModel(unit="Dummy", config={"compiler_options": "foo"})


def test_result_adapter_trace(mock_context: DevAmpelContext):
    from ampel.test.dummy import DummyUnitResultAdapter

    mock_context.register_unit(DummyUnitResultAdapter)
    model = UnitModel(unit="DummyUnitResultAdapter")
    u1 = mock_context.loader.new_context_unit(model, mock_context, run_id=1)
    u2 = mock_context.loader.new_context_unit(model, mock_context, run_id=2)
    assert (
        u1._get_trace_content() == u2._get_trace_content()
    ), "trace content is identical for different run_id"
    assert u1._trace_id == u2._trace_id, "trace id is identical for different run_id"
