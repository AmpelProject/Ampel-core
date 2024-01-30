import pytest

from ampel.core.AmpelContext import AmpelContext
from ampel.model.UnitModel import UnitModel


@pytest.mark.usefixtures("_patch_mongo")
def test_validate(core_config):
    ctx = AmpelContext.load(core_config)
    with ctx.loader.validate_unit_models():
        UnitModel(unit="T3LatestStateDataLoader", config={"directives": []})
