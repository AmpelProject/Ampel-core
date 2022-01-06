import pytest
import yaml

from ampel.config.builder.ProcessMorpher import ProcessMorpher
from ampel.config.collector.T02ConfigCollector import T02ConfigCollector


@pytest.fixture
def first_pass_config(testing_config):
    with open(testing_config) as f:
        return yaml.safe_load(f)


@pytest.fixture
def config_collector(first_pass_config, ampel_logger):
    first_pass_config["confid"] = T02ConfigCollector(
        conf_section="confid", logger=ampel_logger
    )
    return first_pass_config


@pytest.mark.parametrize(
    "config,hashed_config",
    [
        ({}, {"test_parameter": 1}),
        ({"test_parameter": 1}, {"test_parameter": 1}),
        ({"test_parameter": 2}, {"test_parameter": 2}),
    ],
)
def test_hash_t2_config(config_collector, config, hashed_config, ampel_logger):

    m = ProcessMorpher(
        {
            "active": True,
            "channel": "HU_RANDOM",
            "controller": {"unit": "ZTFAlertStreamController"},
            "distrib": "ampel-hu-astro",
            "name": "HU_RANDOM|T0|ztf_uw_private",
            "processor": {
                "config": {
                    "compiler_opts": None,
                    "directives": [
                        {
                            "channel": "HU_RANDOM",
                            "filter": {
                                "config": {"passing_rate": 0.0001},
                                "unit": "RandFilter",
                            },
                            "ingest": {
                                "point_t2": [{"unit": "DemoPointT2Unit", "config": config}]
                            },
                        }
                    ],
                    "shaper": {"unit": "ZiDataPointShaper"},
                    "supplier": {
                        "config": {
                            "loader": {
                                "config": {
                                    "bootstrap": "partnership.alerts.ztf.uw.edu:9092",
                                    "group_name": "ampel-v0.7.1",
                                    "stream": "ztf_uw_private",
                                    "timeout": 3600,
                                },
                                "unit": "UWAlertLoader",
                            }
                        },
                        "unit": "ZiAlertSupplier",
                    },
                },
                "unit": "AlertConsumer",
            },
            "schedule": ["super"],
            "source": "/Users/jakob/Documents/ZTF/Ampel-v0.8/Ampel-HU-astro/conf/ampel-hu-astro/channel/HU_RANDOM.yml",
            "tier": 0,
        },
        templates=[],
        logger=ampel_logger,
    )
    m.apply_template(config_collector)

    m.hash_t2_config(config_collector)
    assert (
        len(configs := list(config_collector["confid"].values())) == 1
    ), "config hash emitted"
    assert configs[0] == hashed_config, "validated config appears in output"
