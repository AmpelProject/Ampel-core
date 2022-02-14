import copy, pytest
from ampel.config.builder.ConfigValidator import ConfigValidator
from ampel.template.PeriodicSummaryT3 import PeriodicSummaryT3


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
                "run": {"unit": "DemoReviewT3Unit"},
            }
        )
        .get_process(config, ampel_logger) | {"version": 0}
    )
    # from ampel.core.EventHandler import EventHandler
    # eh = EventHandler("foo", ampel_db=None, tier=3, dry_run=True)
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
                "run": [{"unit": "DemoReviewT3Unit", "config": {}}],
            }
        )
        .get_process(config, ampel_logger) | {"version": 0}
    )
    assert ConfigValidator(config).validate() == config
    assert config["process"]["t3"]["foo"]["channel"] is None
