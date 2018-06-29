
import pytest
import fastavro
import os
import time
from math import isnan
from collections import defaultdict

from ampel import archive

from sqlalchemy import select, create_engine, MetaData
import sqlalchemy
from sqlalchemy.sql.functions import count
from collections import Iterable
import json

@pytest.fixture(scope="function", params=['postgres'])
def mock_database_args(request):
    def pg_drop_database(engine, database):
       # terminate connections and drop database
       with engine.connect() as conn:
           try:
               conn.execute("commit")
               conn.execute(
                   """ALTER DATABASE {db} WITH CONNECTION LIMIT 0;
                      SELECT pg_terminate_backend(sa.pid) FROM pg_stat_activity sa WHERE 
                      sa.pid <> pg_backend_pid() AND sa.datname = '{db}';""".format(db=database))
               conn.execute("commit")
               conn.execute("DROP DATABASE {db};".format(db=database))
           except sqlalchemy.exc.ProgrammingError:
               pass
    if request.param == 'sqlite':
        uri = 'sqlite:///:memory:'
    elif request.param == 'postgres':
        user = 'ampel'
        host = os.environ['ARCHIVE']
        database = 'test_archive'
        password = archive.docker_env('POSTGRES_PASSWORD')
        master = create_engine('postgresql://{}:{}@{}/{}'.format(user,password,host,'postgres'))
        pg_drop_database(master, database)
        with master.connect() as conn:
            conn.execute("commit")
            conn.execute("create database {}".format(database))
        
        uri = 'postgresql://{}:{}@{}/{}'.format(user, password, host, database)
    yield uri

    if request.param == 'postgres':
       pg_drop_database(master, database)

@pytest.fixture
def mock_database(postgres):
    engine = create_engine(postgres)
    meta = MetaData()
    meta.reflect(bind=engine)
    with engine.connect() as connection:
        yield meta, connection

def test_create_database(mock_database):
    meta, connection = mock_database
    assert connection.execute('SELECT COUNT(*) from alert').first()[0] == 0
    assert connection.execute('SELECT alert_version from versions ORDER BY version_id DESC LIMIT 1').first()[0] == 2.0

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

def count_previous_candidates(alert):
    upper_limits = sum((1 for c in alert['prv_candidates'] if c['candid'] is None))
    return len(alert['prv_candidates'])-upper_limits, upper_limits

def test_insert_duplicate_alerts(mock_database, alert_generator):
    import itertools
    processor_id = 0
    meta, connection = mock_database
    
    alert = next(alert_generator())
    detections, upper_limits = count_previous_candidates(alert)
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == detections
    assert connection.execute(count(meta.tables['upper_limit'].columns.upper_limit_id)).first()[0] == upper_limits
    
    # inserting the same alert a second time does nothing
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == detections
    assert connection.execute(count(meta.tables['upper_limit'].columns.upper_limit_id)).first()[0] == upper_limits

def test_insert_duplicate_photopoints(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    from sqlalchemy.sql.expression import tuple_, func
    from sqlalchemy.sql.functions import sum
    
    alert = next(alert_generator())
    detections, upper_limits = count_previous_candidates(alert)
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == detections
    assert connection.execute(count(meta.tables['upper_limit'].columns.upper_limit_id)).first()[0] == upper_limits
    assert connection.execute(count(meta.tables['alert_prv_candidate_pivot'].columns.alert_id)).first()[0] == 1
    assert connection.execute(sum(func.array_length(meta.tables['alert_prv_candidate_pivot'].columns.prv_candidate_id, 1))).first()[0] == detections
    assert connection.execute(count(meta.tables['alert_upper_limit_pivot'].columns.upper_limit_id)).first()[0] == 1
    assert connection.execute(sum(func.array_length(meta.tables['alert_upper_limit_pivot'].columns.upper_limit_id, 1))).first()[0] == upper_limits
    
    # insert a new alert, containing the same photopoints. only the alert and pivot tables should gain entries
    alert['candid'] += 1
    alert['candidate']['candid'] = alert['candid']
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 2
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 2
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == detections
    assert connection.execute(count(meta.tables['upper_limit'].columns.upper_limit_id)).first()[0] == upper_limits
    assert connection.execute(count(meta.tables['alert_prv_candidate_pivot'].columns.alert_id)).first()[0] == 2
    assert connection.execute(sum(func.array_length(meta.tables['alert_prv_candidate_pivot'].columns.prv_candidate_id, 1))).first()[0] == 2*detections
    assert connection.execute(count(meta.tables['alert_upper_limit_pivot'].columns.upper_limit_id)).first()[0] == 2
    assert connection.execute(sum(func.array_length(meta.tables['alert_upper_limit_pivot'].columns.upper_limit_id, 1))).first()[0] == 2*upper_limits

def test_delete_alert(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    from sqlalchemy.sql.expression import tuple_, func
    from sqlalchemy.sql.functions import sum
    
    alert = next(alert_generator())
    detections, upper_limits = count_previous_candidates(alert)
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))

    Alert = meta.tables['alert']
    connection.execute(Alert.delete().where(Alert.c.candid==alert['candid']))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 0
    assert connection.execute(count(meta.tables['candidate'].columns.candid)).first()[0] == 0
    assert connection.execute(count(meta.tables['alert_prv_candidate_pivot'].columns.alert_id)).first()[0] == 0
    assert connection.execute(sum(func.array_length(meta.tables['alert_prv_candidate_pivot'].columns.prv_candidate_id, 1))).first()[0] == None
    assert connection.execute(count(meta.tables['alert_upper_limit_pivot'].columns.upper_limit_id)).first()[0] == 0
    assert connection.execute(sum(func.array_length(meta.tables['alert_upper_limit_pivot'].columns.upper_limit_id, 1))).first()[0] == None
    # array-joined tables don't participate in delete cascade, because ELEMENT REFERENCES is still not a thing
    # http://blog.2ndquadrant.com/postgresql-9-3-development-array-element-foreign-keys/
    assert connection.execute(count(meta.tables['prv_candidate'].columns.candid)).first()[0] == detections
    assert connection.execute(count(meta.tables['upper_limit'].columns.upper_limit_id)).first()[0] == upper_limits

def assert_alerts_equivalent(alert, reco_alert):
    
    # some necessary normalization on the alert
    fluff = ['pdiffimfilename', 'programpi', 'ssnamenr']
    alert = dict(alert)
    def strip(in_dict):
        out_dict = dict(in_dict)
        for k,v in in_dict.items():
            if isinstance(v, float) and isnan(v):
                out_dict[k] = None
            if k in fluff:
                del out_dict[k]
            if k == 'isdiffpos' and v is not None:
                assert v in {'0', '1', 'f', 't'}
                out_dict[k] = v in {'1', 't'}
        return out_dict
    alert['candidate'] = strip(alert['candidate'])
    # fluff += ['rbversion']
    assert alert.keys() == reco_alert.keys()
    for k in alert:
        if 'candidate' in k:
            continue
        assert alert[k] == pytest.approx(reco_alert[k])
    assert len(alert['prv_candidates']) == len(reco_alert['prv_candidates'])
    prvs = sorted(alert['prv_candidates'], key=lambda f: (f['jd'], f['candid'] is None))
    reco_prvs = sorted(reco_alert['prv_candidates'], key=lambda f: (f['jd'], f['candid'] is None))
    try:
        assert [c['candid'] for c in prvs] == [c['candid'] for c in reco_prvs]
    except:
        jd_off = lambda cands: [c['jd'] - cands[0]['jd'] for c in cands]
        print(jd_off(prvs))
        print(jd_off(reco_alert['prv_candidates']))
        raise
    for prv, reco_prv in zip(prvs, reco_prvs):
        prv = strip(prv)
        assert sorted(prv.keys()) == sorted(reco_prv.keys())
        for k in prv.keys():
            # print(k, prv[k], reco_prv[k])
            try:
                assert prv[k] == pytest.approx(reco_prv[k])
            except:
                print(k, prv[k], reco_prv[k])
                raise
        assert prv == pytest.approx(reco_prv)
        #assert prv == reco_prv
    assert alert['candidate'] == pytest.approx(reco_alert['candidate'])

def test_get_cutout(mock_database, cutout_alert_generator, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    
    for idx, alert in enumerate(alert_generator()):
        processor_id = idx % 16
        archive.insert_alert(connection, meta, alert, processor_id, 0)
        cutout = archive.get_cutout(connection, meta, alert['candid'])
        assert cutout == {}

    for idx, alert in enumerate(cutout_alert_generator()):
        processor_id = idx % 16
        archive.insert_alert(connection, meta, alert, processor_id, 0)

    for idx, alert in enumerate(cutout_alert_generator()):
        processor_id = idx % 16
        cutouts = archive.get_cutout(connection, meta, alert['candid'])
        alert_cutouts = {k[len('cutout'):].lower() : v['stampData'] for k,v in alert.items() if k.startswith('cutout')}
        assert cutouts == alert_cutouts


    

def test_get_alert(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database
    
    timestamps = []
    jds = defaultdict(dict)
    for idx, alert in enumerate(alert_generator()):
        processor_id = idx % 16
        timestamps.append(int(time.time()*1e6))
        assert alert['candid'] not in jds[alert['candidate']['jd']]
        jds[alert['candidate']['jd']][alert['candid']] = (processor_id,alert)
        archive.insert_alert(connection, meta, alert, processor_id, timestamps[-1])

    exposures = sorted(jds.keys())
    assert len(exposures) == 4
    jd_min = exposures[1]
    jd_max = exposures[3]
    reco_jds = {exposures[i]: {k: pair[1] for k,pair in jds[exposures[i]].items()} for i in (1,2)}

    # retrieve alerts in the middle two exposures
    for reco_alert in archive.get_alerts_in_time_range(connection, meta, jd_min, jd_max):
        alert = reco_jds[reco_alert['candidate']['jd']].pop(reco_alert['candid'])
        assert_alerts_equivalent(alert, reco_alert)
    for k in reco_jds.keys():
        assert len(reco_jds[k]) == 0, "retrieved all alerts in time range"

    # retrieve again, but only in a subset of partitions
    reco_jds = {exposures[i]: {k: pair[1] for k,pair in jds[exposures[i]].items() if (pair[0] >= 5 and pair[0] < 12)} for i in (1,2)}
    for reco_alert in archive.get_alerts_in_time_range(connection, meta, jd_min, jd_max, slice(5,12)):
        alert = reco_jds[reco_alert['candidate']['jd']].pop(reco_alert['candid'])
        assert_alerts_equivalent(alert, reco_alert)
    for k in reco_jds.keys():
        assert len(reco_jds[k]) == 0, "retrieved all alerts in time range"

    hit_list = []
    for i,alert in enumerate(alert_generator()):
        reco_alert = archive.get_alert(connection, meta, alert['candid'])
        assert_alerts_equivalent(alert, reco_alert)
        if i % 17 == 0:
            hit_list.append(alert)

    for i,reco_alert in enumerate(archive.get_alerts(connection, meta, [c['candid'] for c in hit_list])):
        alert = hit_list[i]
        assert_alerts_equivalent(alert, reco_alert)

def test_archive_object(alert_generator, postgres, mock_database):
    db = archive.ArchiveDB(postgres)
    
    from itertools import islice
    for alert, schema in islice(alert_generator(with_schema=True), 10):
        db.insert_alert(alert, schema, 0, 0)
    
    for alert in islice(alert_generator(), 10):
        reco_alert = db.get_alert(alert['candid'])
        # some necessary normalization on the alert
        for k,v in alert['candidate'].items():
            if isinstance(v, float) and isnan(v):
                alert['candidate'][k] = None
        assert_alerts_equivalent(alert, reco_alert)

def test_insert_future_schema(alert_generator, postgres, mock_database_args):
    engine = create_engine(mock_database_args)
    archive.create_database(alert_schema, engine)
    db = archive.ArchiveDB(mock_database_args)

    alert, schema = next(alert_generator(True))
    schema['version'] = str(float(schema['version'])+.1)
    with pytest.raises(ValueError) as e_info:
        db.insert_alert(alert, schema, 0, 0)

