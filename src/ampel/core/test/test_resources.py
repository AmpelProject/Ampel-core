
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
import pytest
from urllib import parse

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

def test_default_resource():
	uri = "mongodb://foo:bar@hostymchostington:27018"
	AmpelConfig.reset()
	AmpelConfig.set_config({'resources': {'mongo': {'writer': uri}}})
	
	assert AmpelConfig.get_config('resources.mongo.writer') == uri

def test_override_mongo():
	uri = "mongodb://foo:bar@hostymchostington:27018"
	AmpelConfig.reset()
	config = {'resources': {'mongo': {'writer': uri}}}
	AmpelConfig.set_config(config)
	
	import pkg_resources
	from configargparse import ArgumentParser
	
	entry = next(pkg_resources.iter_entry_points('ampel.pipeline.resources', 'mongo'), None)
	if entry is None:
		raise NameError("Resource {} is not defined".format(name))
	resource = entry.resolve()
	
	parser = ArgumentParser()
	default = resource.parse_default(AmpelConfig.get_config('resources'))
	resource.add_arguments(parser, default, ['writer'])
	opts = parser.parse_args(args=['--mongo-host', 'blergh'])
	
	mongo = resource.parse_args(opts)
	
	assert len(mongo) == 1
	parse.urlparse(mongo['writer']).hostname == 'blergh'

def test_override_graphite():
	uri = "graphite://foo:bar@hostymchostington:27018"
	AmpelConfig.reset()
	config = {'resources': {'graphite': uri}}
	AmpelConfig.set_config(config)
	
	import pkg_resources
	from configargparse import ArgumentParser
	
	entry = next(pkg_resources.iter_entry_points('ampel.pipeline.resources', 'graphite'), None)
	if entry is None:
		raise NameError("Resource {} is not defined".format(name))
	resource = entry.resolve()
	
	parser = ArgumentParser()
	default = resource.parse_default(AmpelConfig.get_config('resources'))
	resource.add_arguments(parser, default, [])
	opts = parser.parse_args(args=['--graphite-host', 'blergh'])
	
	graphite = resource.parse_args(opts)
	
	assert isinstance(graphite['default'], str)
	parse.urlparse(graphite['default']).hostname == 'blergh'
	

def test_argumentparser():
	AmpelConfig.reset()
	parser = AmpelArgumentParser(tier=None)
	parser.require_resource('mongo', ['writer'])
	
	uri = 'mongodb://foo:bar@testirific:2001/'
	parts = parse.urlparse(uri)
	
	parser.parse_args(args=['--mongo-host', parts.hostname, '--mongo-port', str(parts.port), '--mongo-writer-username', parts.username, '--mongo-writer-password', parts.password])
	
	assert AmpelConfig.get_config('resources.mongo.writer') == uri

def test_slack():
	AmpelConfig.reset()
	parser = AmpelArgumentParser(tier=None)
	parser.require_resource('slack', ['operator'])
	
	parser.parse_args(args=['--slack-operator-token', 'foo'])
	
	assert AmpelConfig.get_config('resources.slack.operator') == 'foo'
	
