
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

@pytest.mark.parametrize("config_source,alert_source", [("env", "tarball"), ("env", "archive"), ("cmdline", "tarball"), ("cmdline", "archive")])
def test_alertprocessor_entrypoint(alert_tarball, empty_mongod, postgres, graphite, config_source, alert_source):
	if alert_source == "tarball":
		cmd = ['ampel-alertprocessor', '--tarfile', alert_tarball, '--channels', 'HU_RANDOM']
	elif alert_source == "archive":
		cmd = ['ampel-alertprocessor', '--archive', '2000-01-01', '2099-01-01', '--channels', 'HU_RANDOM']
	if config_source == "env":
		env = {**resource_env(empty_mongod, 'mongo', 'writer'),
		       **resource_env(postgres, 'archive', 'writer'),
		       **resource_env(graphite, 'graphite'),
		       'SLOT': '1'}
		env.update(os.environ)
	elif config_source == "cmdline":
		env = os.environ
		cmd += resource_args(empty_mongod, 'mongo', 'writer') \
		    + resource_args(postgres, 'archive', 'writer') \
		    + resource_args(graphite, 'graphite')
		if alert_source == "archive":
			cmd += ['--slot', '1']
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

def test_ingestion_from_archive(empty_archive, alert_generator, minimal_ingestion_config):
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	from ampel.archive import ArchiveDB
	from ampel.pipeline.t0.AlertProcessor import AlertProcessor
	from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from itertools import islice

	db = ArchiveDB(empty_archive)
	for idx, (alert, schema) in enumerate(islice(alert_generator(with_schema=True), 100)):
		db.insert_alert(alert, schema, idx%16, 0)

	alerts = db.get_alerts_in_time_range(-float('inf'), float('inf'), programid=1)
	supplier = AlertSupplier(alerts, alert_shaper=ZIAlertShaper())

	ap = AlertProcessor(publish_stats=[])
	iter_count = ap.run(supplier)
	assert iter_count == idx+1

