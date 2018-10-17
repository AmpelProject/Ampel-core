
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ConfigLoader import ConfigLoader
from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.pipeline.t3.T3Controller import T3Controller

import logging

def test_validate_config(mocker):

	mocker.patch('pymongo.MongoClient')
	mocker.patch('extcats.CatalogQuery.CatalogQuery')

	AmpelConfig.set_config(
        ConfigLoader.load_config(gather_plugins=True)
    )
	
	for channel_config in ChannelConfigLoader.load_configurations(None, 0):
		pass
	
	assert len(T3Controller.load_job_configs(None, logging.getLogger())) > 0
