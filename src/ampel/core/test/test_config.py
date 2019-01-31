
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.ConfigLoader import ConfigLoader
from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.pipeline.t3.T3Controller import T3Controller
from ampel.pipeline.t3.T3Task import T3Task
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.t3.T3Job import T3Job
from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig
from unittest.mock import MagicMock

import pytest
import logging

@pytest.fixture
def t3_unit_mocker(mocker):
	patched = set()
	def patch(unit):
		if not unit in patched:
			klass = AmpelUnitLoader.get_class(3, unit)
			mock = mocker.patch('{}.{}'.format(klass.__module__, klass.__name__))
			AmpelUnitLoader.UnitClasses[3][unit] = mock
			patched.add(unit)
		return AmpelUnitLoader.UnitClasses[3][unit]
	yield patch
	AmpelUnitLoader.UnitClasses[3].clear()

@pytest.fixture
def mock_mongo(mocker):
	mocker.patch('pymongo.MongoClient')
	yield
	# clean up cached mock collections
	AmpelDB.reset()

def test_validate_config(mocker, mock_mongo, t3_unit_mocker):

	mocker.patch('extcats.CatalogQuery.CatalogQuery')
	log = logging.getLogger()

	AmpelConfig.set_config(
        ConfigLoader.load_config(gather_plugins=True)
    )
	
	for channel_config in ChannelConfigLoader.load_configurations(None, 0):
		pass

	for channel_config in ChannelConfigLoader.load_configurations(None, "all"):
		pass
	
	job_configs = T3Controller.load_job_configs(None)
	assert len(job_configs)
	kwargs = {
		'logger': log,
		'db_logging': False,
		'update_tran_journal': False,
		'update_events': False,
		'raise_exc': True,
	}
	
	get_channels = mocker.patch('ampel.pipeline.t3.T3Job.T3Job._get_channels')
	get_channels.return_value = ['FOO', 'BAR', 'BAZ']
	for name, config in job_configs.items():
		if isinstance(config, T3JobConfig):
			for task in config.tasks:
				t3_unit_mocker(task.unitId)
			T3Job(config, **kwargs)
		else:
			t3_unit_mocker(task.unitId)
			T3Task(config, **kwargs)
	
