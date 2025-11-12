import copy

import pytest

from ampel.config.alter.HashT2Config import HashT2Config
from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.test.DummyStateT2Unit import DummyStateT2Unit


def test_load_old_configids(mock_context: DevAmpelContext, ampel_logger):
    """
    Configuration hashes stored via HashT2Config.alter are loaded from the
    database when a new DevAmpelContext is instantiated
    """
    unit_config = {"foo": 37}
    mock_context.register_unit(DummyStateT2Unit)
    pre_register_context = DevAmpelContext(
        config=AmpelConfig(copy.deepcopy(mock_context.config.get())),
        db=mock_context.db,
        loader=mock_context.loader,
    )

    altered = HashT2Config().alter(
        mock_context,
        dict(
            directives=[
                dict(
                    channel="TEST",
                    ingest=dict(
                        combine=[
                            dict(
                                unit="T1SimpleCombiner",
                                state_t2=[dict(unit="DummyStateT2Unit", config=unit_config)],
                            )
                        ]
                    )
                )
            ]
        ),
        ampel_logger
    )

    hashed_unit_config = altered['directives'][0]['ingest']['combine'][0]['state_t2'][0]['config']
    assert isinstance(hashed_unit_config, int)

    post_register_context = DevAmpelContext(
        config=AmpelConfig(copy.deepcopy(pre_register_context.config.get())),
        db=mock_context.db,
        loader=mock_context.loader,
    )
    # config was not present before registration
    with pytest.raises(ValueError, match=r"Config with id .* not found"):
        pre_register_context.config.get_conf_by_id(hashed_unit_config)

    # config is present after registration in the context where it was registered
    unit_config_from_confid = mock_context.config.get_conf_by_id(hashed_unit_config)
    assert unit_config_from_confid == unit_config

    # also in an unrelated context instantiated after registration
    assert (
        post_register_context.config.get_conf_by_id(hashed_unit_config) == unit_config
    ), "unregistered configs loaded from database"
