
from ampel.abstract.AbsTargetSource import AbsTargetSource

import pytest
import logging
from astropy import units as u
from astropy.time import Time
from numpy import random, arccos
random.seed(0)
import asyncio

class PotemkinTargetSource(AbsTargetSource):
	
	version = 0.1
	
	def __init__(self, idx):
		self.idx = idx
	
	async def get_targets(self):
		for i in range(2):
			await asyncio.sleep(random.uniform(0, 1))
			t0 = Time.now()
			yield ((random.uniform(-180, 180)*u.deg, (arccos(random.uniform(0, 1))*u.rad).to(u.deg)), 0.1*u.deg*self.idx, (t0-5*u.day, t0+25*u.day), ['NO_FILTER'])

from ampel.archive import ArchiveDB

@pytest.fixture
def archivedb():
	uri = 'postgresql://ampel-readonly:XXXXXXXXX@localhost:5432/ztfarchive'
	try:
		ArchiveDB(uri)
		return uri
	except OperationalError as e:
		raise e
		return pytest.skip("No archive db found")

from ampel.pipeline.config.AmpelConfig import AmpelConfig
@pytest.fixture
def testing_config(mongod, archivedb):
	AmpelConfig.reset()
	config = {
	    'global': {
	        "sources": {
	            "ZTFIPAC": {
	                "alerts": {
	                    "alertHistoryLength": 30,
	                    "processing": {
	                        "parse": "ampel.pipeline.t0.alerts.ZIAlertParser",
	                        "shape": "ampel.pipeline.t0.alerts.ZIAlertShaper",
	                        "serialization": "avro"
	                    },
	                    "flags": [
	                        "INST_ZTF",
	                        "SRC_IPAC"
	                    ],
	                    "mappings": {
	                        "tranId": "objectId",
	                        "pptId": "candid",
	                        "obsDate": "jd",
	                        "filterId": "fid",
	                        "mag": "magpsf"
	                    }
	                },
	                "ingestion": {
	                    "class": "ampel.pipeline.t0.ingesters.ZIAlertIngester",
	                    "photoShaper": "ampel.pipeline.t0.ingesters.ZIPhotoPointShaper",
	                    "checkReprocessing": True,
	                    "tranIdPrefix": None
	                },
	                "flags": {
	                    "photo": "INST_ZTF",
	                    "main": "INST_ZTF"
	                }
	            }
	        }
	    },
	    'resources': {
	        'mongo': {'writer': mongod},
	        'archive': {'reader': archivedb},
	        },
	    'channels': {
	        "NO_FILTER": {
	            "version": 1.0,
	            "active": False,
	            "sources": {
	                "ZTFIPAC": {
	                    "parameters": {
	                        "ZTFPartner": True,
	                        "autoComplete": True,
	                        "updatedHUZP": True
	                    },
	                    "t0Filter": {
	                        "dbEntryId": "NoFilter",
	                        "runConfig": {}
	                    },
	                    "t2Compute": [],
	                    "t3Supervize": {}
	                }
	            },
	            "general": {
	                "t3Supervize": {
	                    "Purge": {}
	                }
	            }
	        },
	    },
	    't0_filters' : {
	        'NoFilter' : {
	            'classFullPath': 'ampel.contrib.hu.t0.NoFilter',
	        },
	    },
	    't2_units': {},
	}
	AmpelConfig.set_config(config)
	yield config
	AmpelConfig.reset()

from ampel.pipeline.t0.DelayedT0Controller import DelayedT0Controller

def test_source():
	ts = PotemkinTargetSource(0)
	
	async def getem():
		async for t in ts.get_targets():
			assert isinstance(t, tuple)
	asyncio.get_event_loop().run_until_complete(getem())

def test_delayed_controller(testing_config, caplog):
	caplog.set_level('INFO')
	
	controller = DelayedT0Controller([PotemkinTargetSource(i) for i in range(3)])
	
	controller.run()
