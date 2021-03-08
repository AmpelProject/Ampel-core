import copy

import pytest
from ampel.config.builder.ConfigValidator import ConfigValidator
from ampel.model.template.PeriodicSummaryT3 import PeriodicSummaryT3


@pytest.mark.parametrize("loader_directives", [None, [{"col": "stock"}], ["TRANSIENT"]])
def test_validate(core_config, loader_directives, ampel_logger):
    """
    PeriodicSummaryT3 emits a valid process.
    """
    config = copy.deepcopy(core_config)
    config["process"]["t3"]["foo"] = (
        PeriodicSummaryT3(
            **{
                "name": "foo",
                "schedule": "every().day.at('15:00')",
                "channel": "FOO",
                "load": loader_directives,
                "run": {"unit": "DemoT3Unit"},
            }
        )
        .get_process(ampel_logger)
    )
    assert ConfigValidator(config).validate() == config

def test_single_element_run_sequence(core_config, ampel_logger):
    config = copy.deepcopy(core_config)
    config["process"]["t3"]["foo"] = (
        PeriodicSummaryT3(
            **{
                "name": "foo",
                "schedule": "every().day.at('15:00')",
                "channel": {"any_of": ["HU_GP_10", "HU_GP_59"]},
                "load": ["TRANSIENT", "DATAPOINT", "T2RECORD"],
                "run": [{"unit": "DemoT3Unit", "config": {}}],
            }
        )
        .get_process(ampel_logger)
    )
    assert ConfigValidator(config).validate() == config
    assert config["process"]["t3"]["foo"]["channel"] is None
