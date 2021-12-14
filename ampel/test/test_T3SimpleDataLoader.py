from ampel.t3.supply.load.T3SimpleDataLoader import T3SimpleDataLoader
from ampel.core.AmpelContext import AmpelContext


def test_instantiate(core_config, patch_mongo, ampel_logger):
    """
    AbsT3Loader understands all the aliases in the ampel-core config
    """
    ctx = AmpelContext.load(core_config)
    aliases = ctx.config.get("alias.t3", dict)
    assert len(
        directives := T3SimpleDataLoader(
            context=ctx,
            logger=ampel_logger,
            directives=[k[1:] for k in aliases.keys()]
        ).directives
    ) == len(aliases)
    for d, value in zip(directives, aliases.values()):
        assert d.dict(exclude_defaults=True) == value
