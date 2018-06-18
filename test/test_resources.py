
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser
import pytest

def test_initial_state():
	AmpelConfig.reset()
	with pytest.raises(RuntimeError):
		AmpelConfig.get_config(str())

@pytest.fixture
def config():
	AmpelConfig.reset()
	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer'])
	parser.parse_args(args=[])
	
	return AmpelConfig

def test_mongo_resource(config):
	assert config.get_config('resources.mongo')()['writer'].startswith('mongodb://')
