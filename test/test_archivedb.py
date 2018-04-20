
import pytest
import fastavro
import os
import time
from math import isnan

from ampel import archive

from sqlalchemy import select, create_engine
from sqlalchemy.sql.functions import count
from collections import Iterable
import json

@pytest.fixture
def alert_schema():
    parent = os.path.dirname(os.path.realpath(__file__)) + '/../'
    with open(parent+'alerts/schema.json', 'rb') as f:
        return json.load(f)

@pytest.fixture(scope="function", params=['sqlite', 'postgres'])
def mock_database(alert_schema, request):
    meta = archive.create_metadata(alert_schema)
    if request.param == 'sqlite':
        engine = archive.create_database(meta, 'sqlite:///:memory:')
    elif request.param == 'postgres':
        user = 'ampel'
        host = os.environ['ARCHIVE']
        database = 'test_archive'
        password = archive.docker_env('POSTGRES_PASSWORD')
        master = create_engine('postgresql://{}:{}@{}/{}'.format(user,password,host,'postgres'))
        with master.connect() as conn:
            conn.execute("commit")
            conn.execute("create database {}".format(database))
        
        uri = 'postgresql://{}:{}@{}/{}'.format(user, password, host, database)
        engine = archive.create_database(meta, uri)
    connection = engine.connect()
    yield meta, connection

    if request.param == 'postgres':
        # terminate connections and drop database
        with master.connect() as conn:
            conn.execute("commit")
            conn.execute(
                """ALTER DATABASE {db} WITH CONNECTION LIMIT 0;
                   SELECT pg_terminate_backend(sa.pid) FROM pg_stat_activity sa WHERE 
                   sa.pid <> pg_backend_pid() AND sa.datname = '{db}';""".format(db=database))
            conn.execute("commit")
            conn.execute("DROP DATABASE {db};".format(db=database))

def test_create_database(alert_schema):
    meta = archive.create_metadata(alert_schema)
    engine = archive.create_database(meta, 'sqlite:///:memory:', echo=True)
    connection = engine.connect()
    assert connection.execute('SELECT COUNT(*) from alert').first()[0] == 0

def test_insert_unique_alerts(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    timestamps = []
    candids = set()
    for alert in alert_generator():
        # alerts are unique
        assert alert['candid'] not in candids
        candids.add(alert['candid'])
        
        # (candid,pid) is unique within an alert packet
        prevs = dict()
        for idx, candidate in enumerate([alert['candidate']] + alert['prv_candidates']):
            key = (candidate['candid'], candidate['pid'])
            assert key not in prevs
            prevs[key] = candidate
        
        timestamps.append(int(time.time()*1e6))
        archive.insert_alert(connection, meta, alert, processor_id, timestamps[-1])
    rows = connection.execute(select([meta.tables['alert'].c.ingestion_time])).fetchall()
    db_timestamps = sorted([tup[0] for tup in rows])
    assert timestamps == db_timestamps

def test_insert_duplicate_alerts(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    
    alert = next(alert_generator())
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == len(alert['prv_candidates'])
    
    # inserting the same alert a second time does nothing
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == len(alert['prv_candidates'])

def test_insert_duplicate_photopoints(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    
    alert = next(alert_generator())
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == len(alert['prv_candidates'])
    assert connection.execute(count(meta.tables['alert_prv_candidate_pivot'].columns.candid)).first()[0] == len(alert['prv_candidates'])
    
    # insert a new alert, containing the same photopoints. only the alert and pivot tables should gain entries
    alert['candid'] += 1
    alert['candidate']['candid'] = alert['candid']
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 2
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 2
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == len(alert['prv_candidates'])
    assert connection.execute(count(meta.tables['alert_prv_candidate_pivot'].columns.candid)).first()[0] == 2*(len(alert['prv_candidates']))

def test_get_alert(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    
    timestamps = []
    for alert in alert_generator():
        timestamps.append(int(time.time()*1e6))
        archive.insert_alert(connection, meta, alert, processor_id, timestamps[-1])
    
    for alert in alert_generator():
        reco_alert = archive.get_alert(connection, meta, alert['candid'])
        # some necessary normalization on the alert
        for k,v in alert['candidate'].items():
            if isinstance(v, float) and isnan(v):
                alert['candidate'][k] = None
        assert alert.keys() == reco_alert.keys()
        for k in alert:
            if 'candidate' in k:
                continue
            assert alert[k] == pytest.approx(reco_alert[k])
        assert len(alert['prv_candidates']) == len(reco_alert['prv_candidates'])
        for prv, reco_prv in zip(alert['prv_candidates'], reco_alert['prv_candidates']):
            assert prv == pytest.approx(reco_prv)
        assert alert['candidate'] == pytest.approx(reco_alert['candidate'])

def test_archive_object(alert_generator, alert_schema):
    import tempfile
    dbfile = tempfile.mktemp()
    try:
        meta = archive.create_metadata(alert_schema)
        engine = archive.create_database(meta, 'sqlite:///{}'.format(dbfile))
        db = archive.ArchiveDB('sqlite:///{}'.format(dbfile))
    
        from itertools import islice
        for alert in islice(alert_generator(), 10):
            db.insert_alert(alert, 0, 0)
    
        for alert in islice(alert_generator(), 10):
            reco_alert = db.get_alert(alert['candid'])
            # some necessary normalization on the alert
            for k,v in alert['candidate'].items():
                if isinstance(v, float) and isnan(v):
                    alert['candidate'][k] = None
            assert alert == reco_alert
    finally:
        os.unlink(dbfile)
