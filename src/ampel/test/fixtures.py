
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

@pytest.fixture(scope='session')
def alert_tarball():
	return join(dirname(__file__), '..', '..', '..', 'alerts', 'recent_alerts.tar.gz')

@pytest.fixture(scope='session')
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
	return alerts

@pytest.fixture(scope='session')
def lightcurve_generator(alert_generator):
	from ampel.utils.ZIAlertUtils import ZIAlertUtils
	def lightcurves():
		for alert in alert_generator():
			lightcurve = ZIAlertUtils.to_lightcurve(content=alert)
			assert isinstance(lightcurve.get_photopoints(), tuple)
			yield lightcurve

	return lightcurves

@pytest.fixture(scope='session')
def transientview_generator(alert_generator):
	from ampel.utils.ZIAlertUtils import ZIAlertUtils
	from ampel.base.ScienceRecord import ScienceRecord
	from datetime import datetime
	from numpy import random
	def views():
		for alert in alert_generator():
			results = [
				{
					'versions': {'py': 1.0, 'run_config': 1.0},
					'dt': datetime.utcnow().timestamp(),
					'duration': 0.001,
					'results': {'foo': random.uniform(0,1), 'bar': random.uniform(0,1)}
				}
				for _ in range(random.poisson(1))
			]
			for r in results:
				if random.binomial(1, 0.5):
					del r['results']
					r['error'] = 512
			records = [ScienceRecord(alert['objectId'], 'FancyPants', None, results)]
			tw = ZIAlertUtils.to_transientview(content=alert, science_records=records)
			yield tw

	return views

@pytest.fixture
def ampel_alerts(alert_generator):
	from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from ampel.base.AmpelAlert import AmpelAlert
	def ampelize(shaped_alert):
		return AmpelAlert(shaped_alert['tran_id'], shaped_alert['ro_pps'], shaped_alert['ro_uls'])
	yield map(ampelize, AlertSupplier(alert_generator(), ZIAlertShaper()))

@pytest.fixture
def latest_schema():
	with open(join(dirname(__file__), '..', '..', '..', 'alerts', 'schema_2.0.json')) as f:
		return json.load(f)

import copy
from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
from ampel.base.AmpelAlert import AmpelAlert
class AlertFactoryFixture(object):
	def __init__(self, schema):
		native_types = {'int': int, 'long': int, 'float': float, 'double': float, 'string': str}
		def get_defaults(schema):
			required = dict()
			default = dict()
			for f in schema['fields']:
				if 'default' in f:
					default[f['name']] = f['default']
				elif isinstance(f['type'], dict):
					required[f['name']] = get_defaults(f['type'])
				else:
					required[f['name']] = native_types[f['type']]
			default.update(required)
			return default
		alert = get_defaults(schema)
		def set(mapping, k, v):
			if isinstance(mapping[k], type):
				assert isinstance(v, mapping[k])
			mapping[k] = v
		set(alert, 'schemavsn', schema['version'])
		set(alert, 'publisher', 'zardoz')
		set(alert, 'candid', 1001)
		set(alert, 'objectId', 'ZTF10aaaaaaaa')
		
		self._template = alert
		self._shaper = ZIAlertShaper()
	def __call__(self, **kwargs):
		alert = copy.deepcopy(self._template)
		defaults = {
			'jd': 2458276.6933449,
			'fid': 2,
			'pid': 522193343615,
			'diffmaglim': 20.660852432250977,
			'programid': 1,
			'candid': 522193343615015004,
			'isdiffpos': 't',
			'tblid': 4,
			'nid': 522,
			'rcid': 36,
			'field': 477,
			'xpos': 1176.86962890625,
			'ypos': 2029.61376953125,
			'ra': 214.2322306,
			'dec': 5.8014371,
			'magpsf': 19.52411460876465,
			'sigmapsf': 0.09479492902755737,
			'chipsf': 8.641731262207031,
			'magap': 19.622299194335938,
			'sigmagap': 0.1695999950170517,
			'distnr': 9.204261779785156,
			'magnr': 22.899999618530273,
			'sigmagnr': 0.3330000042915344,
			'chinr': 0.9470000267028809,
			'sharpnr': -0.15600000321865082,
			'sky': -0.06626152992248535,
			'magdiff': 0.09818483889102936,
			'fwhm': 2.1700000762939453,
			'classtar': 1.0,
			'mindtoedge': 1050.88623046875,
			'magfromlim': 1.0385527610778809,
			'seeratio': 1.0293431282043457,
			'aimage': 0.5260000228881836,
			'bimage': 0.38100001215934753,
			'aimagerat': 0.2423963099718094,
			'bimagerat': 0.17557603120803833,
			'elong': 1.3805774450302124,
			'nneg': 5,
			'nbad': 0,
			'rb': 0.25999999046325684,
			'ssdistnr': -999.0,
			'ssmagnr': -999.0,
			'ssnamenr': 'null',
			'sumrat': 0.7873558402061462,
			'magapbig': 19.44849967956543,
			'sigmagapbig': 0.1850000023841858,
			'ranr': 214.2333544,
			'decnr': 5.8037704,
			'sgmag1': 22.059799194335938,
			'srmag1': 22.004600524902344,
			'simag1': 21.603599548339844,
			'szmag1': 21.28969955444336,
			'sgscore1': 0.9783329963684082,
			'distpsnr1': 10.150647163391113,
			'ndethist': 1,
			'ncovhist': 3,
			'jdstarthist': 2458276.6933449,
			'jdendhist': 2458276.6933449,
			'scorr': 26.1721801757812,
			'tooflag': 0,
			'objectidps1': 114962142293772824,
			'objectidps2': 114952142289005042,
			'sgmag2': 20.21619987487793,
			'srmag2': 19.808000564575195,
			'simag2': 19.678800582885742,
			'szmag2': 19.605600357055664,
			'sgscore2': 1.0,
			'distpsnr2': 24.609607696533203,
			'objectidps3': 114952142250229131,
			'sgmag3': -999.0,
			'srmag3': 21.59040069580078,
			'simag3': 20.763399124145508,
			'szmag3': 20.44099998474121,
			'sgscore3': 0.0,
			'distpsnr3': 27.410842895507812,
			'nmtchps': 5,
			'rfid': 477120236,
			'jdstartref': 2458186.957326,
			'jdendref': 2458258.750185,
			'nframesref': 15}
		alert['candidate'].update(defaults)
		alert['candidate'].update(kwargs)
		shaped = self._shaper.shape(alert)
		return AmpelAlert(shaped['tran_id'], shaped['ro_pps'], shaped['ro_uls'])

@pytest.fixture
def alert_factory(latest_schema):
	return AlertFactoryFixture(latest_schema)
	
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
	from ampel.core.flags.AlDocTypes import AlDocTypes
	
	tran_col = AmpelDB.get_collection('main')
	assert tran_col.count({'alDocType': AlDocTypes.TRANSIENT}) == len(choices), "Transient docs exist for all ingested alerts"
	assert max(num_pps) > 0, "At least 1 photopoint was ingested"
	
	return dict(choices)

@pytest.fixture
def t3_selected_transients(ingested_transients, minimal_ingestion_config, caplog):
	from ampel.pipeline.t3.T3Job import T3Job
	from ampel.pipeline.t3.T3JobConfig import T3JobConfig
	from ampel.pipeline.db.DBContentLoader import DBContentLoader
	
	job = T3Job(T3JobConfig.load('jobbyjob'))
	trans_cursor = job.get_selected_transients()
	assert trans_cursor.count() == len(ingested_transients), "Job loaded all ingested transients"
	
	with caplog.at_level('WARN'):
		loader = DBContentLoader(job.tran_col.database, logger=job.logger)
	chunk = next(job.get_tran_data(loader, trans_cursor, trans_cursor.count()))
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
				channels=task_chans,
				t2_ids=set()
			)
	views = list(filter(lambda k: k is not None, map(create_view, t3_selected_transients.values())))
	assert len(views) > 0, "At least 1 view created"
	assert max(map(lambda tv: len(tv.photopoints), views)) > 0, "At least 1 photopoint in a view"
	
	return views

