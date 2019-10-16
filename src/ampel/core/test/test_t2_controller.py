
import pytest
from ampel.base.abstract.AbsT2Unit import *
from ampel.t2.T2Controller import T2Controller
from ampel.db.AmpelDB import AmpelDB

class PotemkinError(RuntimeError):
	pass

class PotemkinT2(AbsT2Unit):
	
	version = 1.0
	
	def __init__(self, logger : Logger, base_config : Optional[Dict[str,str]] = None):
		"""
		:param logger: logger to use for reporting output
		:param base_config: resources configured for this unit. The keys are
		    the elements of :py:attr:`resources`.
		"""
		pass

	def run(self, light_curve, run_config):
		"""
		Process a lightcurve for a single transient
		
		:returns: dictionary of results
		"""
		raise PotemkinError

from os.path import abspath, join, dirname
import pytest

@pytest.fixture(scope='session')
def alert_tarball():
	return join(dirname(__file__), '..', '..', 'ampel-ztf', 'alerts', 'ztf_public_20180819_mod1000.tar.gz')

@pytest.fixture(scope='session')
def alert_generator(alert_tarball):
	import itertools
	import fastavro
	from ampel.t0.load.TarballWalker import TarballWalker
	def alerts(with_schema=False):
		atat = TarballWalker(alert_tarball)
		for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
			reader = fastavro.reader(fileobj)
			alert = next(reader)
			if with_schema:
				yield alert, reader.schema
			else:
				yield alert
	return alerts

@pytest.fixture
def potemkin_t2():
	
	T2Controller.t2_classes['POTEMKIN'] = PotemkinT2
	T2Controller.add_version('POTEMKIN', 'py', PotemkinT2)


@pytest.mark.skip("t2 be broke yo")
def test_t2_error_reporting(potemkin_t2, ingested_transients):
	
	troubles = AmpelDB.get_collection('troubles', 'r')
	count = troubles.count({})
	controller = T2Controller()
	controller.t2_run_config['POTEMKIN_default'] = None
	controller.process_new_docs()
	assert troubles.count({}) == count + len(ingested_transients)
	
	doc = troubles.find_one({})
	assert doc['more'] == 1

def test_get_required_resources():
	from ampel.t2.T2Controller import get_required_resources
	from ampel.config.ConfigLoader import ConfigLoader
	
	from ampel.config.AmpelConfig import AmpelConfig
	AmpelConfig.reset()
	try:
		AmpelConfig.set_config(ConfigLoader.load_config(tier="all"))
		assert len(AmpelConfig.get('channel')) > 0
		resources = get_required_resources()
		assert len(resources) > 0
	finally:
		AmpelConfig.reset()
