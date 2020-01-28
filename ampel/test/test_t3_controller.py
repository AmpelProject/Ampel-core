import pytest, subprocess, json, schedule

from ampel.t3.T3Controller import T3Controller
from ampel.config.t3.T3JobConfig import T3JobConfig
from ampel.t3.T3Job import T3Job

from ampel.core.AmpelUnitLoader import AmpelUnitLoader

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.config.AmpelConfig import AmpelConfig
from ampel.common.AmpelUtils import AmpelUtils
from ampel.run.AmpelArgumentParser import AmpelArgumentParser
from argparse import Namespace

from ampel.pipeline.t3.T3PlaceboUnit import T3PlaceboUnitError

@pytest.fixture
def t3_jobs():
    return [ {
      "job": "jobbyjob",
      "active": True,
      "schedule": "every().minute",
      "transients": {
        "state": "$latest",
        "select": {
          "created" : {
                "after" : {
                  "use": "$timeDelta",
                  "arguments": {"days": -40}
                }
            },
            "modified" : {
                "after" : {
                  "use": "$timeDelta",
                  "arguments": {"days": -1}
                }
            },
            "channels": {'anyOf': [
              "NUCLEAR_CHARLOTTE",
              "NUCLEAR_SJOERT"
            ]},
            "withTags": "INST_ZTF",
            "withoutTags": "HAS_ERROR"
        },
        "content": {
          "docs": [
            "TRANSIENT",
            "COMPOUND",
            "T2RECORD",
            "PHOTOPOINT"
          ],
          "t2SubSelection": [
            "SNCOSMO",
            "AGN"
          ]
        },
        "chunk": 200
      },
      "tasks": [
        {
          "task": "SpaceCowboy",
          "className": "T3PlaceboUnit",
          "runConfig": {"raise": True},
          "transients" : {
            "select": {
                "channels": {
                    "anyOf": [
                        "NUCLEAR_CHARLOTTE",
                        "NUCLEAR_SJOERT"
                    ]
                }
            },
            "content": {
                "t2SubSelection": "SNCOSMO"
            }
          }
        }
      ]
    }
    ]

@pytest.fixture
def testing_config(t3_jobs, mongod, graphite):
	AmpelConfig.reset()
	config = {
	    'global': {},
	    'resources': {
	        'mongo': {'writer': mongod, 'logger': mongod},
	        'graphite': {'default': graphite},
	     },
	    't3Jobs': {job['job']: job for job in t3_jobs},
	    't3Tasks': {},
	}
	AmpelConfig.set_config(config)
	return config

def test_launch_job(testing_config):
	from ampel.db.AmpelDB import AmpelDB

	# invoke job directly
	troubles = AmpelDB.get_collection('troubles').count_documents({})
	job = T3Job(T3JobConfig(**AmpelConfig.get_config('t3Jobs.jobbyjob')))
	job.run()
	assert AmpelDB.get_collection('troubles').count_documents({}) == troubles+1, "an exception was logged"

	# start job in subprocess via T3Controller
	controller = T3Controller(['jobbyjob'])
	fut = controller.launch_t3_job(controller.job_configs['jobbyjob'])
	fut.get(10)
	assert AmpelDB.get_collection('troubles').count_documents({}) == troubles+2, "an exception was logged"

def test_monitor_processes(testing_config):
	controller = T3Controller()
	controller.launch_t3_job(controller.job_configs['jobbyjob'])
	stats = controller.monitor_processes()
	assert stats['processes'] == 1
	controller.join()

@pytest.fixture
def minimal_config(mongod):
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
		't2Units': {},
		'channels': dict(map(make_channel, range(2))),
		't3Jobs' : {
			'jobbyjob': {
				'job': 'jobbyjob',
				'schedule': 'every(1).hour',
				'transients': {
					'select':  {
						'channels': {'anyOf': ['0', '1']},
					},
					'content': {
						'docs': ['TRANSIENT', 'COMPOUND', 'T2RECORD', 'PHOTOPOINT']
					},
					'state': '$all'
				},
				'tasks': [
					{'task': 'noselect', 'className': 'T3PlaceboUnit'},
					{'task': 'config', 'className': 'T3PlaceboUnit', 'runConfig': {}},
					# {'name': 'badconfig', 't3Unit': 'potemkin', 'runConfig': 'default_doesnotexist'},
					{'task': 'select0', 'className': 'T3PlaceboUnit', 'transients': {'select': {'channels': '0'}, 'state': '$all'}},
					{'task': 'select1', 'className': 'T3PlaceboUnit', 'transients': {'select': {'channels': '1'}, 'state': '$latest'}},
					
				]
			}
		},
		't3Tasks': {}
	}
	AmpelConfig.set_config(config)
	yield config
	AmpelConfig.reset()

def test_missing_job(minimal_config):
	assert AmpelConfig.get("t3Jobs.jobbyjob_doesnotexist") is None

@pytest.mark.skip('Depends on ampel-ztf')
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
	controller = T3Controller(['jobbyjob'])
	assert len(controller.scheduler.jobs) > 1
	job = controller.scheduler.jobs[0]
	assert job.unit == 'hours'
	assert job.interval == 1
	assert 'jobbyjob' in job.tags

def test_schedule_malicious_job():
	import schedule
	from ampel.config.t3.ScheduleEvaluator import ScheduleEvaluator
	scheduler = schedule.Scheduler()
	ev = ScheduleEvaluator()

	good = [
		"every(10).minutes",
		"every().hour",
		"every().day.at('10:30')",
		"every().monday",
		"every().wednesday.at('13:15')",
	]
	for line in good:
		assert isinstance(ev(scheduler, line), schedule.Job)

	bad = [
		"raise ValueError",
		"import sys; sys.exit(1)",
		"with open('foo.txt', 'w') as f:\n\tf.write('seekrit')"
	]
	for line in bad:
		with pytest.raises(ValueError):
			ev(scheduler, line)

def test_entrypoint_list(testing_config, capsys):
	from ampel.t3.T3Controller import list_tasks

	list_tasks(Namespace())
	captured = capsys.readouterr()
	assert len(captured.out.split('\n'))-4 == 1

def test_entrypoint_show(testing_config, capsys):
	from ampel.t3.T3Controller import show

	show(Namespace(job='jobbyjob', task=None))
	captured = capsys.readouterr()
	assert len(captured.out.split('\n')) == 70

	show(Namespace(job='jobbyjob', task='SpaceCowboy'))
	captured = capsys.readouterr()
	assert len(captured.out.split('\n')) == 21

def test_entrypoint_runjob(testing_config, capsys):
	from ampel.db.AmpelDB import AmpelDB
	from ampel.t3.T3Controller import runjob

	troubles = AmpelDB.get_collection('troubles').count()

	with pytest.raises(T3PlaceboUnitError):
		runjob(Namespace(job='jobbyjob', task=None, update_run_col=True, update_tran_journal=True))

def test_entrypoint_rununit(testing_config, capsys):
	from ampel.db.AmpelDB import AmpelDB
	from ampel.t3.T3Controller import rununit

	troubles = AmpelDB.get_collection('troubles').count()
	
	with pytest.raises(T3PlaceboUnitError):
		rununit(Namespace(
			unit='T3PlaceboUnit',
			created=-1,
			modified=-1,
			channels=['0', '1'],
			science_records=None,
			runConfig={'raise': True},
			update_tran_journal=False,
			update_run_col=False,
			chunk=10
		))

def test_get_required_resources():
	from ampel.t3.T3Controller import get_required_resources, T3Controller, T3JobConfig, AmpelUnitLoader
	from ampel.config.ConfigLoader import ConfigLoader
	from ampel.config.AmpelConfig import AmpelConfig
	
	AmpelConfig.set_config(ConfigLoader.load_config(tier="all"))
	assert len(AmpelConfig.get("t3Jobs")) > 0
	assert len(T3Controller.load_job_configs()) > 0
	units = set()
	for job in T3Controller.load_job_configs().values():
		if isinstance(job, T3JobConfig):
			for task in job.tasks:
				units.add(task.className)
		else:
			task = job
			units.add(task.className)
	assert len(units) > 0
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(3, unit).resources:
			resources.add(resource)
	assert len(resources) > 0

	resources = get_required_resources()
	assert len(resources) > 0

def test_time_selection():
	from ampel.query.QueryMatchStock import QueryMatchStock
	from ampel.t3.TimeConstraintBuilder import TimeConstraintBuilder
	from ampel.config.time.TimeConstraintConfig import TimeConstraintConfig
	from ampel.config.time.TimeDeltaConfig import TimeDeltaConfig
	
	channels = {'anyOf': ['DESY_NEUTRINO', 'HU_SNSAMPLE']}
	created = TimeConstraint(TimeConstraintConfig(after=dict(use='$timeDelta', arguments=dict(days=-30))))
	modified = TimeConstraint(TimeConstraintConfig(after=dict(use='$timeDelta', arguments=dict(days=-2))))
	
	query = QueryMatchStock.build_query(channels, time_created=created, time_modified=modified)
	assert len(query['$or']) == len(channels['anyOf']), "OR on time constraints for all channels"
	for or_clause in query['$or']:
		assert list(or_clause.keys()) == ['$and'], "each element is an AND clause"
		assert len(next(iter(or_clause.values()))) == 2, "each element is an AND clause "


