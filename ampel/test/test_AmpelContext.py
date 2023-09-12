import copy

from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.test.DummyStateT2Unit import DummyStateT2Unit
from ampel.config.alter.HashT2Config import HashT2Config

def test_load_old_configids(mock_context: DevAmpelContext, ampel_logger):
    """ This test unit might no longer be required or might need a rename """
    unit_config = {"foo": 37}
    mock_context.register_unit(DummyStateT2Unit)
    pre_register_context = DevAmpelContext(
        config=AmpelConfig(copy.deepcopy(mock_context.config.get())),
        db=mock_context.db,
        loader=mock_context.loader,
    )

    HashT2Config().alter(
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

    post_register_context = DevAmpelContext(
        config=AmpelConfig(copy.deepcopy(pre_register_context.config.get())),
        db=mock_context.db,
        loader=mock_context.loader,
    )
    assert (
        isinstance(
            pre_register_confid := pre_register_context.config.get("confid", dict), dict
        )
        and len(pre_register_confid) == 0
    ), "config not present before registration"

    assert (
        isinstance(confid := mock_context.config.get("confid", dict), dict)
        and len(confid) == 1
    ), "config present after registration"
    assert next(iter(confid.values())) == unit_config
    assert (
        post_register_context.config.get("confid", dict) == confid
    ), "unregistered configs loaded from database"
