from typing import Any, Dict, List, TYPE_CHECKING

import pytest
import yaml

from ampel.model.template.AbsLegacyChannelTemplate import AbsLegacyChannelTemplate

if TYPE_CHECKING:
    from ampel.log.AmpelLogger import AmpelLogger
    from ampel.config.builder.FirstPassConfig import FirstPassConfig
    from ampel.model.UnitModel import UnitModel

class LegacyChannelTemplate(AbsLegacyChannelTemplate):
    # Mandatory implementation
    def get_processes(
        self, logger: "AmpelLogger", first_pass_config: "FirstPassConfig"
    ) -> List[Dict[str, Any]]:

        ret: List[Dict[str, Any]] = []

        t0_ingester = "DummyAlertContentIngester"
        t1_ingester = "DummyCompoundIngester"
        t2_compute_from_t0 = self.t2_compute
        t2_compute_from_t1 : List["UnitModel"] = []
        ret.insert(
            0,
            self.craft_t0_process(
                first_pass_config,
                controller={},
                stock_ingester="StockIngester",
                t0_ingester=t0_ingester,
                t1_ingester=t1_ingester,
                t1_standalone_ingester=None,
                t2_state_ingester="DummyStateT2Ingester",
                t2_point_ingester="DummyPointT2Ingester",
                t2_stock_ingester="DummyStockT2Ingester",
                t2_compute_from_t0=t2_compute_from_t0,
                t2_compute_from_t1=t2_compute_from_t1,
            ),
        )

        return ret


@pytest.fixture
def first_pass_config(testing_config):
    with open(testing_config, "rb") as f:
        return yaml.safe_load(f)


@pytest.mark.parametrize(
    "t2_compute,target,expected",
    [
        # single, statebound T2
        (
            [{"unit": "DummyStateT2Unit"}],
            ["t0_add", "t1_combine", 0, "t2_compute", "units"],
            {"unit": "DummyStateT2Unit"},
        ),
        # statebound T2 with explicit statebound dependency
        (
            [
                {
                    "unit": "DummyTiedStateT2Unit",
                    "config": {"dependency": [{"unit": "DummyStateT2Unit"}]},
                }
            ],
            ["t0_add", "t1_combine", 0, "t2_compute", "units"],
            {"unit": "DummyStateT2Unit"},
        ),
        # statebound T2 with implicit (default) statebound dependency
        pytest.param(
            [{"unit": "DummyTiedStateT2Unit"}],
            ["t0_add", "t1_combine", 0, "t2_compute", "units"],
            {"unit": "DummyStateT2Unit"},
            marks=pytest.mark.xfail(reason="default dependencies aren't automatically resolved")
        ),
        # statebound T2 with statebound dependency, also explicitly configured
        (
            [
                {"unit": "DummyStateT2Unit"},
                {
                    "unit": "DummyTiedStateT2Unit",
                    "config": {"dependency": [{"unit": "DummyStateT2Unit"}]},
                },
            ],
            ["t0_add", "t1_combine", 0, "t2_compute", "units"],
            {"unit": "DummyStateT2Unit"},
        ),
        # statebound T2 with point dependency
        (
            [
                {
                    "unit": "DummyTiedStateT2Unit",
                    "config": {"dependency": [{"unit": "DummyPointT2Unit"}]},
                }
            ],
            ["t0_add", "t2_compute", "units"],
            {"unit": "DummyPointT2Unit"},
        ),
        # statebound T2 with stock dependency
        (
            [
                {
                    "unit": "DummyTiedStateT2Unit",
                    "config": {"dependency": [{"unit": "DummyStockT2Unit"}]},
                }
            ],
            ["t2_compute", "units"],
            {"unit": "DummyStockT2Unit"},
        ),
    ],
)
def test_state_t2_instantiation(t2_compute, target, expected, first_pass_config):
    """
    Template creates state T2s (and dependencies)
    """
    processes = LegacyChannelTemplate(
        **{
            "channel": "foo",
            "auto_complete": False,
            "t0_filter": {"unit": "DummyFilter"},
            "t2_compute": t2_compute,
        }
    ).get_processes(None, first_pass_config)
    assert processes
    proc = processes[0]
    assert proc["tier"] == 0
    assert (directives := proc["processor"]["config"]["directives"])
    directive = directives[0]

    def get(item, keys):
        while keys:
            item = item[keys.pop(0)]
        return item

    items = get(directive, target)
    assert expected in items
    assert (
        len([i for i in items if expected == i]) == 1
    ), "exactly one instance of each unit"
