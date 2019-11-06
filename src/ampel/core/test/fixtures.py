from os.path import abspath, join, dirname
from os import environ
import pytest, json, subprocess, socket

def docker_service(image, port, environ={}, mounts=[], healthcheck=None, port_mapping=None):
	container = None
	try:
		cmd = ['docker', 'run', '-d', '--restart', 'always']
		if port_mapping is None:
			cmd += ['-P']
		else:
			for item in port_mapping.items():
				cmd += ['-p', '{}:{}'.format(*item)]
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
			for i in range(120*2):
				if up():
					break
				time.sleep(0.5)
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
	if 'MONGO_HOSTNAME' in environ and 'MONGO_PORT' in environ:
		yield 'mongodb://{}:{}'.format(environ['MONGO_HOSTNAME'], environ['MONGO_PORT'])
	else:
		gen = docker_service('mongo:3.6', 27017)
		port = next(gen)
		yield 'mongodb://localhost:{}/'.format(port)

@pytest.fixture(scope="session")
def graphite():
	if 'GRAPHITE_HOSTNAME' in environ and 'GRAPHITE_PORT' in environ:
		yield 'graphite://{}:{}'.format(environ['GRAPHITE_HOSTNAME'], environ['GRAPHITE_PORT'])
	else:
		gen = docker_service('gographite/go-graphite:latest', 2003)
		port = next(gen)
		yield 'graphite://localhost:{}/'.format(port)

@pytest.fixture(scope="session")
def postgres():
	if 'ARCHIVE_HOSTNAME' in environ and 'ARCHIVE_PORT' in environ:
		yield 'postgresql://ampel@{}:{}/ztfarchive'.format(environ['ARCHIVE_HOSTNAME'], environ['ARCHIVE_PORT'])
	else:
		gen = docker_service('postgres:10.3', 5432,
			environ={'POSTGRES_USER': 'ampel', 'POSTGRES_DB': 'ztfarchive', 'ARCHIVE_READ_USER': 'archive-readonly', 'ARCHIVE_WRITE_USER': 'ampel-client'},
			mounts=[(join(abspath(dirname(__file__)), 'deploy', 'production', 'initdb', 'archive'), '/docker-entrypoint-initdb.d/')],
			healthcheck='pg_isready -U postgres -p 5432 -h `hostname` || exit 1')
		port = next(gen)
		yield 'postgresql://ampel@localhost:{}/ztfarchive'.format(port)

@pytest.fixture
def empty_archive(postgres):
	"""
	Yield archive database, dropping all rows when finished
	"""
	from sqlalchemy import select, create_engine, MetaData

	engine = create_engine(postgres)
	meta = MetaData()
	meta.reflect(bind=engine)
	try:
		with engine.connect() as connection:
			for name, table in meta.tables.items():
				if name != 'versions':
					connection.execute(table.delete())
		yield postgres
	finally:
		with engine.connect() as connection:
			for name, table in meta.tables.items():
				if name != 'versions':
					connection.execute(table.delete())

import copy
from ampel.base.AmpelAlert import AmpelAlert
class AlertFactoryFixture(object):
	def __init__(self, schema):
		from ampel.t0.load.ZIAlertShaper import ZIAlertShaper
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
	from ampel.config.AmpelConfig import AmpelConfig
	from ampel.common.AmpelUnitLoader import AmpelUnitLoader
	from ampel.db.AmpelDB import AmpelDB
	
	AmpelConfig.reset()
	AmpelUnitLoader.reset()
	source = {
			"stream": 'ZTFIPAC',
			"parameters": {
				"ZTFPartner": True,
				"autoComplete": False,
				"updatedHUZP": False
			},
			"t0Filter" : {
				"className": "BasicFilter",
				"runConfig": {
					"operator": ">",
					"len": 1,
					"criteria" : [{
						"attribute": "obsDate",
						"operator": ">",
						"value": 0
					}]
				},
			},
			"t2Compute" : []
	}
	make_channel = lambda name: (str(name), {'channel': name, 'active': True, 'sources': [source]})
	config = {
		'resources': {'mongo': {'writer': mongod, 'logger': mongod}},
		't2Units': {},
		'channels': dict(map(make_channel, range(2))),
		't0Filters' : {
			'BasicFilter': {
				'classFullPath': 'ampel.t0.filter.BasicFilter'
			}
		},
		't3Jobs' : {
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
	for collection in AmpelDB._ampel_cols.keys():
		AmpelDB.get_collection(collection).drop()
	yield config
	AmpelConfig.reset()
	AmpelUnitLoader.reset()

@pytest.fixture
def ingested_transients(alert_generator, minimal_ingestion_config, caplog):
	"""
	Ingest alertsw tih 
	"""
	from ampel.config.AmpelConfig import AmpelConfig
	from ampel.t0.load.AlertSupplier import AlertSupplier
	from ampel.t0.load.ZIAlertShaper import ZIAlertShaper
	from ampel.t0.ingest.ZIAlertIngester import ZIAlertIngester
	from ampel.config.channel.T0Channel import T0Channel
	from bson import ObjectId
	
	import numpy
	numpy.random.seed(0)
	
	from ampel.db.AmpelDB import AmpelDB
	
	# TODO: fix this: T0Channel __init__(self, chan_config, source, logger): where :param chan_config: instance of ampel.config.ChannelConfig
	channels = [T0Channel(str(i), {'sources': AmpelConfig.get('global.sources')}, 'ZTFIPAC', lambda *args: True, set()) for i in range(2)]
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
	
	from ampel.db.AmpelDB import AmpelDB
	from ampel.core.flags.AlDocType import AlDocType
	
	assert AmpelDB.get_collection('stock').find({}).count() == len(choices), "Transient docs exist for all ingested alerts"
	assert max(num_pps) > 0, "At least 1 photopoint was ingested"
	
	return dict(choices)

@pytest.fixture
def t3_selected_transients(ingested_transients, minimal_ingestion_config, caplog):
	from ampel.t3.T3Job import T3Job
	from ampel.t3.T3JobConfig import T3JobConfig
	from ampel.db.DBContentLoader import DBContentLoader
	
	job = T3Job(T3JobConfig.load('jobbyjob'))
	trans_cursor = job.get_selected_transients()
	assert trans_cursor.count() == len(ingested_transients), "Job loaded all ingested transients"
	
	with caplog.at_level('WARN'):
		loader = DBContentLoader(job.col_tran.database, logger=job.logger)
	chunk = next(job.get_tran_data(loader, trans_cursor, trans_cursor.count()))
	assert len(chunk) == len(ingested_transients), "Chunk contains all ingested transients"
	
	assert isinstance(chunk, dict), "get_chunks returns a dict"
	assert max(map(lambda v: len(v.photopoints), chunk.values())) > 0, "At least 1 photopoint was loaded"
	return chunk

@pytest.fixture
def t3_transient_views():
	from os.path import dirname, join
	from ampel.utils.json_serialization import load
	with open(join(dirname(__file__), 'test-data', 'transient_views.json')) as f:
		views = [v for v in load(f)]
	return views

