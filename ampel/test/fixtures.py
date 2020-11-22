from os.path import abspath, join, dirname
from os import environ
import pytest, json, subprocess, socket


@pytest.fixture(scope="session")
def mongod():
	if 'MONGO_HOSTNAME' in environ and 'MONGO_PORT' in environ:
		yield 'mongodb://{}:{}'.format(environ['MONGO_HOSTNAME'], environ['MONGO_PORT'])
	else:
		pytest.skip("No Mongo instance configured")

@pytest.fixture(scope="session")
def graphite():
	if 'GRAPHITE_HOSTNAME' in environ and 'GRAPHITE_PORT' in environ:
		yield 'graphite://{}:{}'.format(environ['GRAPHITE_HOSTNAME'], environ['GRAPHITE_PORT'])
	else:
		pytest.skip("No Graphite instance configured")

@pytest.fixture
def t3_transient_views():
	from os.path import dirname, join
	from ampel.util.json import load
	with open(join(dirname(__file__), 'test-data', 'transient_views.json')) as f:
		views = [v for v in load(f)]
	return views

