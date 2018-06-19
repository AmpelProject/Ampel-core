
import pytest
from ampel.pipeline.t3.T3TaskLoader import T3TaskLoader
from ampel.pipeline.t3.T3Controller import T3Controller
from ampel.pipeline.t3.T3JobLoader import T3JobLoader

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser

import subprocess, json

class PotemkinError(RuntimeError):
	pass

class PotemkinT3(AbsT3Unit):
	
	version = 1.0
	
	def __init__(self, logger, base_config=None, run_config=None, global_info=None):
		pass

	def add(self, transients):
		pass

	def run(self):
		raise PotemkinError

@pytest.fixture
def testing_class():
	prev = T3TaskLoader.t3_classes
	T3TaskLoader.t3_classes = {'potemkin': PotemkinT3}
	yield
	T3TaskLoader.t3_classes = prev

@pytest.fixture
def t3_jobs():
    return {"jobbyjob": {
      "active": True,
      "schedule": {
        "mode" : "fixed_interval",
        "interval" : 1
      },
      "onError": {
        "sendMail": {
          "to": "ztf-software@desy.de",
          "excStack": True
        },
        "stopAmpel": False,
        "retry": False
      },
      "input": {
        "select": {
          "created" : {
                "from" : {
                    "timeDelta" : {
                        "days" : -40
                    }
                }
            },
            "modified" : {
                "from" : {
                    "timeDelta" : {
                        "days" : -1
                    }
                }
            },
            "channel(s)": [
              "NUCLEAR_CHARLOTTE",
              "NUCLEAR_SJOERT"
            ],
            "withFlag(s)": "INST_ZTF",
            "withoutFlag(s)": "HAS_ERROR"
        },
        "load": {
          "state": "$latest",
          "doc(s)": [
            "TRANSIENT",
            "COMPOUND",
            "T2RECORD",
            "PHOTOPOINT"
          ],
          "t2(s)": [
            "SNCOSMO",
            "AGN"
          ],
        },
        "chunk": 200
      },
      "task(s)": [
        {
          "name": "SpaceCowboy",
          "t3Unit": "potemkin",
          "runConfig": None,
          "updateJournal": True,
          "select": {
            "channel(s)": [
              "NUCLEAR_CHARLOTTE",
              "NUCLEAR_SJOERT"
            ],
            "state(s)": "$latest",
            "t2(s)": "SNCOSMO"
          }
        }
      ]
    }
    }

@pytest.fixture
def testing_config(testing_class, t3_jobs, mongod, graphite):
	AmpelConfig.reset()
	config = {
	    'global': {},
	    'resources': {
	        'mongo': {'writer': mongod},
	        'graphite': graphite,
	        },
	    't3_units': {
	    	'potemkin': {
	    		'classFullPath': 'potemkin'
	    	}
	    },
	    't3_jobs': t3_jobs,
	}
	AmpelConfig.set_config(config)
	return config

@pytest.mark.xfail(reason="Exceptions aren't raised to caller yet")
def test_launch_job(testing_config):
	job = T3JobLoader.load('jobbyjob')
	with pytest.raises(PotemkinError):
		job.run()
	with pytest.raises(PotemkinError):
		proc = job.launch_t3_job()
		proc.join()

def test_monitor_processes(testing_config):
	controller = T3Controller(AmpelConfig.get_config(),
	    mongodb_uri=AmpelConfig.get_config('resources.mongo.writer'))
	try:
		controller.start()
		controller.jobs['jobbyjob'].launch_t3_job()
		stats = controller.monitor_processes()
		assert stats['jobbyjob']['processes'] == 1
	finally:
		controller.stop()
	
