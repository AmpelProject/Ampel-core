
from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.AmpelDB import AmpelDB
from ampel.config.ConfigLoader import ConfigLoader
from ampel.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.abstract.UnitLoader import UnitLoader
from ampel.t3.T3Controller import T3Controller
from ampel.t3.T3Task import T3Task
from ampel.config.t3.T3TaskConfig import T3TaskConfig
from ampel.t3.T3Job import T3Job
from ampel.config.t3.T3JobConfig import T3JobConfig
from unittest.mock import MagicMock

import pytest
import logging
from contextlib import contextmanager

@pytest.fixture
def t3_unit_mocker(mocker):
	patched = set()
	def patch(unit):
		if not unit in patched:
			klass = UnitLoader.get_class(3, unit)
			mock = mocker.patch('{}.{}'.format(klass.__module__, klass.__name__))
			UnitLoader.UnitClasses[3][unit] = mock
			patched.add(unit)
		return UnitLoader.UnitClasses[3][unit]
	yield patch
	UnitLoader.UnitClasses[3].clear()

@pytest.fixture
def default_config():
	AmpelConfig.set_config(ConfigLoader.load_config(gather_plugins=True))
	yield AmpelConfig.get_config()
	AmpelConfig.reset()

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
	
	get_channels = mocker.patch('ampel.t3.T3Job.T3Job._get_channels')
	get_channels.return_value = ['FOO', 'BAR', 'BAZ']
	for name, config in job_configs.items():
		if isinstance(config, T3JobConfig):
			for task in config.tasks:
				t3_unit_mocker(task.className)
			T3Job(config, **kwargs)
		else:
			t3_unit_mocker(task.className)
			T3Task(config, **kwargs)

def test_db_prefix(mongod):
	from ampel.db.AmpelDB import AmpelDB
	from ampel.config.AmpelConfig import AmpelConfig
	from ampel.config.ConfigLoader import ConfigLoader
	import json

	AmpelConfig.reset()
	AmpelDB.reset()
	try:
		# default case
		config = {
			'resources': {
				'mongo': {
					'writer': mongod,
					'logger': mongod,
				}
			}
		}
		AmpelConfig.set_config(ConfigLoader.load_config(json.dumps(config)))
		assert AmpelDB.get_collection('t0').database.name == 'Ampel_data'
		AmpelDB.reset()
		# override default
		config = {
			'AmpelDB': {
				'prefix': 'foo'
			},
			'resources': {
				'mongo': {
					'writer': mongod,
					'logger': mongod,
				}
			}
		}
		AmpelConfig.set_config(ConfigLoader.load_config(json.dumps(config)))
		assert AmpelDB.get_collection('t0').database.name == 'foo_data'
	finally:
		AmpelConfig.reset()
		AmpelDB.reset()

@contextmanager
def mod_env(**kwargs):
	from os import environ
	environ['AMPEL_CONFIG'] = '{"AmpelDB":{"prefix":"foo"}}'
	args = AmpelArgumentParser().parse_args([])
	assert args.config['AmpelDB']['prefix'] == 'foo'

def test_skip_t2_units(default_config, mock_mongo):
	from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig
	from ampel.pipeline.t0.Channel import Channel

	config = ChannelConfig.create(0, **{
		"channel": "HU_TNS_MSIP",
		"sources": [
			{
				"stream": "ZTFIPAC",
				"parameters" : {
					"ZTFPartner" : False,
					"auto_complete" : "live",
					"updatedHUZP" : False
				},
				"t0_filter" : {
					"unitId" : "BasicFilter",
					"run_config": {
						"operator": ">",
						"len": 1,
						"criteria": 1
					}
				},
				"t2_compute" : [ 
					{
						"unitId" : "MARSHALMONITOR",
						"run_config" : "simple"
					},
					{
						"unitId" : "CATALOGMATCH",
						"run_config": "general"
					}
				]
			}
		]
	})
	c = Channel(config, 'ZTFIPAC', logging.getLogger(), {'MARSHALMONITOR'})
	assert c.t2_units == {'CATALOGMATCH'}

def test_decrypt_config():
	from sjcl import SJCL
	AmpelConfig.reset()
	secret = 'flerpyfloo'
	passphrase = 'grax'
	enc_dict = SJCL().encrypt(secret.encode(), passphrase)
	AmpelConfig.set_config({'pwds': [passphrase]})
	assert AmpelConfig.decrypt_config(enc_dict) == secret
