
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ConfigLoader import load_config
from ampel.pipeline.config.ChannelLoader import ChannelLoader

def test_validate_config(mocker):

	mocker.patch('pymongo.MongoClient')
	mocker.patch('extcats.CatalogQuery.CatalogQuery')

	AmpelConfig.set_config(load_config(gather_plugins=True))


	# load channels to catch config bugs
	# instantiate channel as used in filtering
	channels = ChannelLoader("ZTFIPAC", 0).load_channels(None)
	assert len(channels) > 0

	# plain Channel object
	channels = ChannelLoader("ZTFIPAC", 2).load_channels(None)
	
