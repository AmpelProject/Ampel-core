from ampel.t3.load.T3SimpleDataLoader import T3SimpleDataLoader
from ampel.core.AmpelContext import AmpelContext
from ampel.config.AmpelConfig import AmpelConfig


def test_instantiate(core_config, patch_mongo, ampel_logger):
    """
    AbsT3Loader understands all the aliases in the ampel-core config
    """
    ctx = AmpelContext.new(AmpelConfig(core_config))
    aliases = ctx.config.get("alias.t3", dict)
    assert len(
        directives := T3SimpleDataLoader(
            ctx, logger=ampel_logger, directives=[k[1:] for k in aliases.keys()]
        ).directives
    ) == len(aliases)
    for d, value in zip(directives, aliases.values()):
        assert d.dict() == value
