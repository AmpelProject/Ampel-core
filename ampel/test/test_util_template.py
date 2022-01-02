from typing import Any
from ampel.model.UnitModel import UnitModel
import pytest
import yaml

from ampel.model.ingest.T2Compute import T2Compute
from ampel.util.template import check_tied_units, filter_units


@pytest.fixture(scope="session")
def first_pass_config(testing_config):
    with open(testing_config) as f:
        return yaml.safe_load(f)


@pytest.mark.parametrize(
    "dependency", ["DummyPointT2Unit", "DummyStockT2Unit", "DummyStateT2Unit"]
)
def test_dependency_present(first_pass_config: dict[str, Any], dependency: str):
    units = [ # type: ignore[var-annotated]
        T2Compute(
            unit="DummyTiedStateT2Unit",
            config={"t2_dependency": [{"unit": dependency}]},
        ),
        T2Compute(unit=dependency),
    ]
    check_tied_units(units, first_pass_config)


def test_missing_dependency(first_pass_config: dict[str, Any]):
    units = [T2Compute(unit="DummyTiedStateT2Unit")] # type: ignore[var-annotated]
    with pytest.raises(ValueError):
        check_tied_units(units, first_pass_config)


def test_misconfigured_dependency(first_pass_config: dict[str, Any]):
    units = [ # type: ignore[var-annotated]
        T2Compute(
            unit="DummyTiedStateT2Unit",
            config={
                "t2_dependency": [{"unit": "DummyStateT2Unit", "config": {"foo": 37}}]
            },
        ),
        T2Compute(unit="DummyStateT2Unit", config={"foo": 42}),
    ]
    with pytest.raises(ValueError):
        check_tied_units(units, first_pass_config)

@pytest.fixture
def all_units(first_pass_config: dict[str, Any]) -> list[UnitModel]:
    return [UnitModel(unit=name) for name in first_pass_config["unit"].keys()]

def test_filter_units(all_units: list[UnitModel], first_pass_config: dict[str, Any]):
    assert len(filter_units(all_units, "LogicalUnit", first_pass_config)) == 10

