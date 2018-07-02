
from ampel.pipeline.t0.AlertProcessor import AlertProcessor, create_databases
from ampel.archive import ArchiveDB
#from ampel.pipeline.t0.alerts.ZIAlertParser import ZIAlertParser

import os
from glob import glob
import random
from itertools import islice

import pytest
import subprocess
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

@pytest.mark.parametrize("config_source", ("env", "cmdline"))
def test_alertprocessor_entrypoint(alert_tarball, mongod, postgres, graphite, config_source):
	cmd = ['ampel-alertprocessor', '--tarfile', alert_tarball, '--channels', 'HU_RANDOM']
	if config_source == "env":
		env = {**resource_env(mongod, 'mongo', 'writer'),
		       **resource_env(postgres, 'archive', 'writer'),
		       **resource_env(graphite, 'graphite')}
		env.update(os.environ)
	elif config_source == "cmdline":
		env = os.environ
		cmd += resource_args(mongod, 'mongo', 'writer') \
		    + resource_args(postgres, 'archive', 'writer') \
		    + resource_args(graphite, 'graphite')
	subprocess.check_call(cmd, env=env)
