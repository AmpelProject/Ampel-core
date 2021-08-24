from typing import Any
import pytest
import yaml

from ampel.model.ingest.T2Compute import T2Compute
from ampel.util.template import check_tied_units


@pytest.fixture(scope="session")
def first_pass_config(testing_config):
    with open(testing_config) as f:
        return yaml.safe_load(f)


@pytest.mark.parametrize(
    "dependency", ["DummyPointT2Unit", "DummyStockT2Unit", "DummyStateT2Unit"]
)
def test_dependency_present(first_pass_config: dict[str, Any], dependency: str):
    units = [
        T2Compute(
            unit="DummyTiedStateT2Unit",
            config={"t2_dependency": [{"unit": dependency}]},
        ),
        T2Compute(unit=dependency),
    ]
    check_tied_units(units, first_pass_config)


def test_missing_dependency(first_pass_config: dict[str, Any]):
    units = [T2Compute(unit="DummyTiedStateT2Unit")]
    with pytest.raises(ValueError):
        check_tied_units(units, first_pass_config)


def test_misconfigured_dependency(first_pass_config: dict[str, Any]):
    units = [
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
