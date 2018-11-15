
from ampel.pipeline.config.AmpelConfig import AmpelConfig
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

class T3UnitMocker:
	"""
	Mock AbsT3Unit instances, restoring state on deletion
	"""
	def __init__(self, mocker):
		self._mocker = mocker
		self._patched = set()
	def __call__(self, unit):
		if unit in self._patched:
			return AmpelUnitLoader.UnitClasses[3][unit]
		else:
			klass = AmpelUnitLoader.get_class(3, unit)
			patched = self._mocker.patch('{}.{}'.format(klass.__module__, klass.__name__))
			AmpelUnitLoader.UnitClasses[3][unit] = patched
			self._patched.add(unit)
			return AmpelUnitLoader.UnitClasses[3][unit]
	def __del__(self):
		for unit in self._patched:
			del AmpelUnitLoader.UnitClasses[3][unit]

@pytest.fixture
def t3_unit_mocker(mocker):
	yield T3UnitMocker(mocker)

def test_validate_config(mocker, t3_unit_mocker):

	mocker.patch('pymongo.MongoClient')
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
	
	for name, config in job_configs.items():
		if isinstance(config, T3JobConfig):
			for task in config.tasks:
				t3_unit_mocker(task.unitId)
			T3Job(config, **kwargs)
		else:
			t3_unit_mocker(task.unitId)
			T3Task(config, **kwargs)
	
