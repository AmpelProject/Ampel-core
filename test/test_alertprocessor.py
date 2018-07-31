
import pytest
import subprocess
import os
from urllib.parse import urlparse

def resource_args(uri, name, role=None):
	uri = urlparse(uri)
	prefix = '--{}'.format(name)
	args = [prefix+'-host', uri.hostname, prefix+'-port', str(uri.port)]
	if role is not None:
		if uri.username is not None:
			args += [prefix+'-'+role+'-username', uri.username]
		if uri.password is not None:
			args += [prefix+'-'+role+'-password', uri.password]
	return args

def resource_env(uri, name, role=None):
	uri = urlparse(uri)
	prefix = name.upper() + "_"
	env = {prefix+"HOSTNAME": uri.hostname, prefix+"PORT": str(uri.port)}
	if role is not None:
		if uri.username is not None:
			env[prefix+role.upper()+"_USERNAME"] = uri.username
		if uri.password is not None:
			env[prefix+role.upper()+"_PASSWORD"] = uri.password
	return env

@pytest.fixture
def empty_mongod(mongod):
	from pymongo import MongoClient
	mc = MongoClient(mongod)
	mc.drop_database('Ampel_logs')
	yield mongod
	mc.drop_database('Ampel_logs')

@pytest.mark.parametrize("config_source", ("env", "cmdline"))
def test_alertprocessor_entrypoint(alert_tarball, empty_mongod, postgres, graphite, config_source):
	cmd = ['ampel-alertprocessor', '--tarfile', alert_tarball, '--channels', 'HU_RANDOM']
	if config_source == "env":
		env = {**resource_env(empty_mongod, 'mongo', 'writer'),
		       **resource_env(postgres, 'archive', 'writer'),
		       **resource_env(graphite, 'graphite')}
		env.update(os.environ)
	elif config_source == "cmdline":
		env = os.environ
		cmd += resource_args(empty_mongod, 'mongo', 'writer') \
		    + resource_args(postgres, 'archive', 'writer') \
		    + resource_args(graphite, 'graphite')
	subprocess.check_call(cmd, env=env)
	from pymongo import MongoClient
	mc = MongoClient(empty_mongod)
	assert mc['Ampel_logs']['troubles'].count({}) == 0

@pytest.fixture
def live_config():
	from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	AmpelConfig.reset()
	AmpelArgumentParser().parse_args(args=[])
	yield
	AmpelConfig.reset()

def test_private_channel_split(live_config):
	from ampel.pipeline.config.ChannelLoader import ChannelLoader
	
	loader = ChannelLoader(source="ZTFIPAC", tier=0)
	params = loader.get_source_parameters()
	private = {k for k,v in params.items() if v.get('ZTFPartner', False)}
	public = set(params.keys()).difference(private)
	assert len(public) == 1
