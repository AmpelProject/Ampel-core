
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ConfigLoader import ConfigLoader

def test_validate_config(mocker):

	mocker.patch('pymongo.MongoClient')
	mocker.patch('extcats.CatalogQuery.CatalogQuery')

	AmpelConfig.set_config(
        ConfigLoader.load_config(gather_plugins=True)
    )
