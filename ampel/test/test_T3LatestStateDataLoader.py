from ampel.core.AmpelContext import AmpelContext
from ampel.model.UnitModel import UnitModel


def test_validate(core_config, patch_mongo):
    ctx = AmpelContext.load(core_config)
    with ctx.loader.validate_unit_models():
        UnitModel(unit="T3LatestStateDataLoader", config={"directives": []})
