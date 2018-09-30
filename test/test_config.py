
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ConfigLoader import ConfigLoader
from ampel.pipeline.config.ChannelLoader import ChannelLoader
from ampel.pipeline.t3.T3JobConfig import T3JobConfig, T3TaskConfig

def test_validate_config(mocker):

	mocker.patch('pymongo.MongoClient')
	mocker.patch('extcats.CatalogQuery.CatalogQuery')

	AmpelConfig.set_config(
        ConfigLoader.load_config(gather_plugins=True)
    )

	# load channels to catch config bugs
	# instantiate channel as used in filtering
	channels = ChannelLoader("ZTFIPAC", 0).load_channels(None)
	assert len(channels) > 0

	# plain Channel object
	channels = ChannelLoader("ZTFIPAC", 2).load_channels(None)

	for job_name in AmpelConfig.get_config("t3Jobs").keys():
		T3JobConfig.load(job_name, None)
