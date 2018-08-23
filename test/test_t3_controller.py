
import pytest, subprocess, json
import schedule

from ampel.pipeline.t3.T3Controller import T3Controller
from ampel.pipeline.t3.T3JobConfig import T3JobConfig
from ampel.pipeline.t3.T3TaskConfig import T3TaskConfig

from ampel.base.abstract.AbsT3Unit import AbsT3Unit
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser


class PotemkinError(RuntimeError):
	pass

class PotemkinT3(AbsT3Unit):
	
	version = 1.0
	
	def __init__(self, logger, base_config=None, run_config=None, global_info=None):
		pass

	def add(self, transients):
		pass

	def done(self):
		raise PotemkinError

@pytest.fixture
def testing_class():
	prev = T3TaskConfig.t3_classes
	T3TaskConfig.t3_classes = {'potemkin': PotemkinT3}
	yield
	T3TaskConfig.t3_classes = prev

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
		  'verbose': True,
		  'debug': False
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
	        'mongo': {'writer': mongod, 'logger': mongod},
	        'graphite': {'default': graphite},
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
	job = T3JobConfig.load('jobbyjob')
	with pytest.raises(PotemkinError):
		job.run()
	with pytest.raises(PotemkinError):
		proc = job.launch_t3_job()
		proc.join()

def test_monitor_processes(testing_config):
	controller = T3Controller()
	try:
		controller.start()
		controller.job_configs['jobbyjob'].launch_t3_job()
		stats = controller.monitor_processes()
		assert stats['jobbyjob']['processes'] == 1
	finally:
		controller.stop()

@pytest.fixture
def minimal_config(mongod, testing_class):
	AmpelConfig.reset()
	sources = {
		'ZTFIPAC': {
			"parameters": {
				"ZTFPartner": True,
				"autoComplete": False,
				"updatedHUZP": False
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
				'schedule': 'every(1).hour',
				'input': {
					'select':  {
						'channel(s)': ['0', '1'],
					},
					'load': {
						'state': '$latest',
						'doc(s)': ['TRANSIENT', 'COMPOUND', 'T2RECORD', 'PHOTOPOINT']
					}
				},
				'task(s)': [
					{'name': 'noselect', 't3Unit': 'potemkin'},
					{'name': 'config', 't3Unit': 'potemkin', 'runConfig': 'default'},
					# {'name': 'badconfig', 't3Unit': 'potemkin', 'runConfig': 'default_doesnotexist'},
					{'name': 'select0', 't3Unit': 'potemkin', 'select': {'channel(s)': ['0']}},
					{'name': 'select1', 't3Unit': 'potemkin', 'select': {'channel(s)': ['1']}},
					
				]
			}
		},
		't3_run_config' : {
			'potemkin_default': {},
		},
		't3_units': {
			'potemkin': {'classFullPath': 'potemkin'}
		}
	}
	AmpelConfig.set_config(config)
	yield config
	AmpelConfig.reset()

def test_missing_job(minimal_config):
	with pytest.raises(ValueError):
		T3JobConfig.load('jobbyjob_doesnotexist')

def test_missing_task(minimal_config):
	with pytest.raises(ValueError):
		T3TaskConfig.load('theytookrjerbs', 'case1')
	with pytest.raises(ValueError):
		T3TaskConfig.load('jobbyjob', 'doesnotexist')

def test_task_config(minimal_config):
	T3TaskConfig.load('jobbyjob', 'noselect')
	T3TaskConfig.load('jobbyjob', 'config')
	with pytest.raises(ValueError):
		T3TaskConfig.load('jobbyjob', 'badconfig')
	T3TaskConfig.load('jobbyjob', 'select0')

def test_get_transient_view(ingested_transients, t3_selected_transients, minimal_ingestion_config):
	
	# test that OR selection works as expected
	for task_chans in (['0'], ['1'], ['0', '1']):
		count = 0
		for tran_id, tran_data in t3_selected_transients.items():
			tran_view = tran_data.create_view(channels=task_chans, t2_ids=set())
			if tran_view is not None:
				count += 1
				if len(task_chans) == 1:
					assert tran_view.channel == task_chans[0], "Transient view contains requested channel"
					assert tran_view.channel in ingested_transients[tran_view.tran_id], "Requested channel passed in underlying transient data"
				else:
					assert any(c in task_chans for c in tran_view.channel), "Transient view contains at least one of requested channels"
					assert tuple(sorted(tran_view.channel)) == tuple(sorted(ingested_transients[tran_view.tran_id])), "Channel list in view matches actual transient"
					
		assert count == len(list(chans for chans in ingested_transients.values() if any(c in task_chans for c in chans))), "Number of views matches ingested transients passing criteria"

def test_schedule_job(minimal_config):
	job = T3JobConfig.load('jobbyjob')
	scheduler = schedule.Scheduler()
	job.schedule_job(scheduler)

