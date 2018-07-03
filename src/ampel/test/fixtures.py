
import pytest
import json
import subprocess
import socket
from os.path import abspath, join, dirname

def docker_service(image, port, environ={}, mounts=[], healthcheck=None):
	container = None
	try:
		cmd = ['docker', 'run', '-d', '--restart', 'always', '-P']
		for k, v in environ.items():
			cmd += ['-e', '{}={}'.format(k,v)]
		if healthcheck is not None:
			cmd += ['--health-start-period', '1s', '--health-interval', '1s','--health-cmd', healthcheck]
		for source, dest in mounts:
			cmd += ['-v', '{}:{}'.format(source, dest)]
		container = subprocess.check_output(cmd + [image]).strip()
		
		import time
		if healthcheck is not None:
			def up():
				 status = json.loads(subprocess.check_output(['docker', 'container', 'inspect', '-f', '{{json .State.Health.Status}}', container]))
				 return status == "healthy"
			for i in range(120):
				if up():
					break
				time.sleep(1)
		ports = json.loads(subprocess.check_output(['docker', 'container', 'inspect', '-f', '{{json .NetworkSettings.Ports}}', container]))
		yield int(ports['{}/tcp'.format(port)][0]['HostPort'])
	except FileNotFoundError:
		return pytest.skip("Docker fixture requires Docker")
	finally:
		if container is not None:
			subprocess.check_call(['docker', 'container', 'stop', container],
				stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			subprocess.check_call(['docker', 'container', 'rm', '--volumes', container],
				stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

@pytest.fixture(scope="session")
def mongod():
	gen = docker_service('mongo:3.6', 27017)
	port = next(gen)
	yield 'mongodb://localhost:{}/'.format(port)

@pytest.fixture(scope="session")
def graphite():
	gen = docker_service('gographite/go-graphite:latest', 2003)
	port = next(gen)
	yield 'graphite://localhost:{}/'.format(port)

@pytest.fixture(scope="session")
def postgres():
	gen = docker_service('postgres:10.3', 5432,
		environ={'POSTGRES_USER': 'ampel', 'POSTGRES_DB': 'ztfarchive', 'ARCHIVE_READ_USER': 'archive-readonly', 'ARCHIVE_WRITE_USER': 'ampel-client'},
		mounts=[(join(abspath(dirname(__file__)), '..', '..', '..', 'deploy', 'production', 'initdb', 'archive'), '/docker-entrypoint-initdb.d/')],
		healthcheck='psql --username ampel --port 5432 ztfarchive || exit 1')
	port = next(gen)
	yield 'postgresql://ampel@localhost:{}/ztfarchive'.format(port)

@pytest.fixture
def alert_tarball():
	return join(dirname(__file__), '..', '..', '..', 'alerts', 'recent_alerts.tar.gz')

@pytest.fixture
def alert_generator(alert_tarball):
	import itertools
	import fastavro
	from ampel.pipeline.t0.alerts.TarballWalker import TarballWalker
	def alerts(with_schema=False):
		atat = TarballWalker(alert_tarball)
		for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
			reader = fastavro.reader(fileobj)
			alert = next(reader)
			for k in {'cutoutDifference', 'cutoutScience', 'cutoutTemplate'}:
				 del alert[k]
			if with_schema:
				yield alert, reader.schema
			else:
				yield alert
	yield alerts

@pytest.fixture
def minimal_ingestion_config(mongod):
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	
	AmpelConfig.reset()
	sources = {
		'ZTFIPAC': {
			"parameters": {
				"ZTFPartner": True,
				"autoComplete": False,
				"updatedHUZP": False
			},
			"flags": {
				"photo": "INST_ZTF",
				"main": "INST_ZTF"
			}
		}
	}
	make_channel = lambda name: (str(name), {'version': 1, 'sources': sources})
	config = {
		'global': {'sources': sources},
		'resources': {'mongo': {'writer': mongod, 'logger': mongod}},
		't2_units': {},
		'channels': dict(map(make_channel, range(2))),
		't3_jobs' : {
			'jobbyjob': {
				'input': {
					'select':  {
						'channel(s)': ['0', '1'],
					},
					'load': {
						'state': '$latest',
						'doc(s)': ['TRANSIENT', 'COMPOUND', 'T2RECORD', 'PHOTOPOINT']
					}
				},
				'task(s)': []
			}
		}
	}
	AmpelConfig.set_config(config)
	yield config
	AmpelConfig.reset()

@pytest.fixture
def ingested_transients(alert_generator, minimal_ingestion_config, caplog):
	"""
	Ingest alertsw tih 
	"""
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester
	from ampel.pipeline.config.T0Channel import T0Channel
	from bson import ObjectId
	
	import numpy
	numpy.random.seed(0)
	
	channels = [T0Channel(str(i), {'version': 1, 'sources': AmpelConfig.get_config('global.sources')}, 'ZTFIPAC', lambda *args: True, set()) for i in range(2)]
	ingester = ZIAlertIngester(channels)
	ingester.set_log_id(ObjectId())
	choices = []
	num_pps = []
	for shaped_alert in AlertSupplier(alert_generator(), ZIAlertShaper()):
		choice = numpy.random.binomial(1, 0.5, 2).astype(bool)
		if not any(choice):
			continue
		t2s = numpy.where(choice, set(), None)
		with caplog.at_level('WARN'):
			ingester.ingest(shaped_alert['tran_id'], shaped_alert['pps'], shaped_alert['uls'], t2s)
		num_pps.append(len(shaped_alert['pps']))
		choices.append((shaped_alert['tran_id'], [c.name for c,k in zip(channels, choice) if k]))
	
	from ampel.pipeline.db.AmpelDB import AmpelDB
	from ampel.flags.AlDocTypes import AlDocTypes
	
	tran_col = AmpelDB.get_collection('main')
	assert tran_col.count({'alDocType': AlDocTypes.TRANSIENT}) == len(choices), "Transient docs exist for all ingested alerts"
	assert max(num_pps) > 0, "At least 1 photopoint was ingested"
	
	return dict(choices)

@pytest.fixture
def t3_selected_transients(ingested_transients, minimal_ingestion_config, caplog):
	from ampel.pipeline.t3.T3JobExecution import T3JobExecution, DBContentLoader
	from ampel.pipeline.t3.T3JobLoader import T3JobLoader
	
	job = T3JobExecution(T3JobLoader.load('jobbyjob'))
	transients = job.get_selected_transients()
	assert transients.count() == len(ingested_transients), "Job loaded all ingested transients"
	
	with caplog.at_level('WARN'):
		loader = DBContentLoader(job.tran_col.database, logger=job.logger)
	chunk = next(job.get_chunks(loader, transients, transients.count()))
	assert len(chunk) == len(ingested_transients), "Chunk contains all ingested transients"
	
	assert isinstance(chunk, dict), "get_chunks returns a dict"
	assert max(map(lambda v: len(v.photopoints), chunk.values())) > 0, "At least 1 photopoint was loaded"
	return chunk

@pytest.fixture
def t3_transient_views(t3_selected_transients):
	from ampel.pipeline.common.AmpelUtils import AmpelUtils
	
	task_chans = ['0', '1']
	def create_view(tran_data):
		return tran_data.create_view(
				channel=task_chans if not AmpelUtils.is_sequence(task_chans) else None,
				channels=task_chans if AmpelUtils.is_sequence(task_chans) else None,
				t2_ids=set()
			)
	views = list(filter(lambda k: k is not None, map(create_view, t3_selected_transients.values())))
	assert len(views) > 0, "At least 1 view created"
	assert max(map(lambda tv: len(tv.photopoints), views)) > 0, "At least 1 photopoint in a view"
	
	return views

