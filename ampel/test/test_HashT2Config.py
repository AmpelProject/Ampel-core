import copy

from ampel.config.alter.HashT2Config import HashT2Config
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.test.dummy import DummyStateT2Unit, DummyTiedStateT2Unit


def test_recursive_hash(mock_context: DevAmpelContext, ampel_logger):
    """
    HashT2Config.alter should hash nested t2 configs
    """
    unit_config = {
        "t2_dependency": [{"unit": "DummyStateT2Unit", "config": {"foo": 37}}]
    }
    for u in (DummyStateT2Unit, DummyTiedStateT2Unit):
        mock_context.register_unit(u)

    hashed = HashT2Config().alter(
        mock_context,
        dict(
            directives=[
                dict(
                    channel="TEST",
                    ingest=dict(
                        combine=[
                            dict(
                                unit="T1SimpleCombiner",
                                state_t2=[
                                    dict(
                                        unit="DummyTiedStateT2Unit",
                                        config=copy.deepcopy(unit_config),
                                    )
                                ],
                            )
                        ]
                    ),
                )
            ]
        ),
        ampel_logger,
    )

    toplevel_config_id = hashed["directives"][0]["ingest"]["combine"][0]["state_t2"][0][
        "config"
    ]
    assert isinstance(toplevel_config_id, int)
    toplevel_config = mock_context.get_config().get_conf_id(toplevel_config_id)
    nested_config_id = toplevel_config["t2_dependency"][0]["config"]
    assert isinstance(nested_config_id, int)
    assert (
        mock_context.get_config().get_conf_id(nested_config_id)
        == unit_config["t2_dependency"][0]["config"]
    )
