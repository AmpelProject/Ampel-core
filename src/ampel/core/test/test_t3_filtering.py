
import pytest
import logging

def test_t3_filtering_demo(t3_transient_views):
    from mongomock.filtering import filter_applies
    from ampel.utils.json import AmpelEncoder
    # the user supplies a query against results like this, along with a unit id and config name
    q = {
        'fit_acceptable': True,
        'sncosmo_info.success': True,
        'fit_results.z': {'$gt': 0},
        'fit_results.x1': {'$lt': 10}
    }
    t2_unit_id = 'SNCOSMO'
    runConfig = 'default'
    # we flesh it out into a full query
    query = {
            't2records': {
                '$elemMatch': {
                    't2_unit_id': t2_unit_id,
                    'info.runConfig': runConfig,
                    'info.hasError': False,
                    **{'results.-1.output.{}'.format(k): v for k,v in q.items()}
                }
            }
    }
    query = {'$and': [query, query]}

    filtered = [view for view in t3_transient_views if filter_applies(query, AmpelEncoder(lossy=True).default(view))]
    assert len(filtered) == 4
    for view in filtered:
        records = [s for s in view.t2records if s.t2_unit_id == t2_unit_id and not s.info['hasError']]
        assert len(records) == 1
        results = records[0].results[-1]['output']
        assert all((
            results['fit_acceptable'],
            results['sncosmo_info']['success'],
            results['fit_results']['z'] > 0,
            results['fit_results']['x1'] < 10,
        ))

@pytest.fixture
def transients(t3_transient_views):
    from ampel.pipeline.t3.TransientData import TransientData
    from ampel.pipeline.config.ConfigLoader import ConfigLoader
    from ampel.pipeline.config.AmpelConfig import AmpelConfig

    AmpelConfig.set_config(ConfigLoader.load_config())
    def view_to_data(view):
        "Invert TransientData.create_view()"
        data = TransientData(view.tran_id, '$latest', logging.getLogger())
        for pp in view.photopoints:
            data.add_photopoint(pp)
        for ul in view.upperlimits:
            data.add_upperlimit(ul)
        data.set_channels([view.channel])
        data.set_flags(view.flags)
        for entry in view.journal:
            data.add_journal_entry(view.channel, entry)
        for entry in view.t2records:
            data.add_science_record(view.channel, entry)
        assert data.create_view(view.channel).photopoints == set(view.photopoints)
        return data
    return list(map(view_to_data, t3_transient_views))

def test_transient_data_filter(transients, mocker):

    from ampel.pipeline.t3.T3Event import T3Event
    from ampel.pipeline.t3.T3Task import T3Task
    from ampel.pipeline.t3.T3Job import T3Job
    from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
    from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig
    from ampel.pipeline.logging.AmpelLogger import AmpelLogger
    
    mocker.patch('pymongo.MongoClient')
    tran_config={
        'state': '$latest',
        'select': {
            'scienceRecords': {
                'unitId': 'SNCOSMO',
                'match': {
                    'fit_acceptable': True,
                    'sncosmo_info.success': True,
                    'fit_results.z': {'$gt': 0},
                    'fit_results.x1': {'$lt': 10}
                }
            }
        },
        'content': {
            'docs': ['T2RECORD']
        }
    }
    add = mocker.patch('ampel.pipeline.t3.T3PlaceboUnit.T3PlaceboUnit.add')
    task = T3Task(
        T3TaskConfig(task='foo', unitId='T3PlaceboUnit', transients=tran_config),
        logger=AmpelLogger.get_logger(), db_logging=False,
        full_console_logging=True, update_tran_journal=False, 
        update_events=False, raise_exc=True)
    task.process_tran_data(transients)
    assert add.called
    assert len(add.call_args[0][0]) == 4

    add.reset_mock()
    assert not add.called
    job = T3Job(
        T3JobConfig(job='foo',
            schedule='every().sunday',
            transients={
                'state': '$latest',
                'select': {},
                'content': {
                    'docs': ['T2RECORD']
                }
            },
            tasks={
                'task': 'foo',
                'unitId': 'T3PlaceboUnit',
                'transients': tran_config
            }),
        logger=AmpelLogger.get_logger(), db_logging=False,
        full_console_logging=True, update_tran_journal=False, 
        update_events=False, raise_exc=True)
    job.process_tran_data(transients)
    assert add.called
    assert len(add.call_args[0][0]) == 4

def test_t3_match_config():
    from ampel.pipeline.config.t3.ScienceRecordMatchConfig import ScienceRecordMatchConfig
    
    config = {
        'unitId': 'SNCOSMO',
        'match': {
            'fit_acceptable': True,
            'sncosmo_info.success': True,
            'fit_results.z': {'$gt': 0},
            'fit_results.x1': {'$lt': 10}
        }
    }
    ScienceRecordMatchConfig(**config)
    # Throws on unknown operator
    config = {
        'unitId': 'SNCOSMO',
        'match': {
            'fit_results.z': {'$ngt': 0},
        }
    }
    with pytest.raises(ValueError):
        ScienceRecordMatchConfig(**config)
