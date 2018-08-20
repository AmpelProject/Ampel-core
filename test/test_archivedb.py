
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

@pytest.fixture
def temp_database(postgres):
    """
    Yield archive database, dropping all rows when finished
    """
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

@pytest.fixture
def mock_database(temp_database):
    engine = create_engine(temp_database)
    meta = MetaData()
    meta.reflect(bind=engine)
    with engine.connect() as connection:
        yield meta, connection

def test_insert_unique_alerts(temp_database, alert_generator):
    processor_id = 0
    db = archive.ArchiveDB(temp_database)
    connection = db._connection
    meta = db._meta
    timestamps = []
    candids = set()
    for alert, schema in alert_generator(with_schema=True):
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
        db.insert_alert(alert, schema, processor_id, timestamps[-1])
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
    
    # find an alert with at least 1 previous detection
    for alert in alert_generator():
        detections, upper_limits = count_previous_candidates(alert)
        if detections > 0:
            break
    assert detections > 0
    
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
    fluff = ['pdiffimfilename', 'programpi']
    alert = dict(alert)
    def strip(in_dict):
        out_dict = dict(in_dict)
        for k,v in in_dict.items():
            if isinstance(v, float) and isnan(v):
                out_dict[k] = None
            if k in fluff:
                del out_dict[k]
        return out_dict
    alert['candidate'] = strip(alert['candidate'])
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
        # remove keys not in original alert (because it came from an older schema)
        for k in set(reco_prv.keys()).difference(prv.keys()):
            del reco_prv[k]
        assert sorted(prv.keys()) == sorted(reco_prv.keys())
        for k in prv.keys():
            # print(k, prv[k], reco_prv[k])
            try:
                assert prv[k] == pytest.approx(reco_prv[k])
            except:
                print(k, prv[k], reco_prv[k])
                raise
        assert prv == pytest.approx(reco_prv)
    keys = {k for k,v in alert['candidate'].items() if v is not None}
    candidate = {k:v for k,v in alert['candidate'].items() if k in keys}
    reco_candidate = {k:v for k,v in reco_alert['candidate'].items() if k in keys}
    for k in set(alert['candidate'].keys()).difference(keys):
        assert reco_alert['candidate'][k] is None
    assert candidate == pytest.approx(reco_candidate)

def test_get_cutout(mock_database, alert_generator):
    processor_id = 0
    meta, connection = mock_database

    for idx, alert in enumerate(alert_generator()):
        processor_id = idx % 16
        archive.insert_alert(connection, meta, alert, processor_id, 0)

    for idx, alert in enumerate(alert_generator()):
        processor_id = idx % 16
        cutouts = archive.get_cutout(connection, meta, alert['candid'])
        alert_cutouts = {k[len('cutout'):].lower() : v['stampData'] for k,v in alert.items() if k.startswith('cutout')}
        assert cutouts == alert_cutouts

@pytest.mark.skip(reason="Testing alert tarball only contains a single exposure")
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

def test_archive_object(alert_generator, postgres):
    import astropy.units as u
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
    
    alerts = list(islice(alert_generator(), 10))
    candids = [a['candid'] for a in alerts]
    reco_candids = [a['candid'] for a in db.get_alerts(candids)]
    assert reco_candids == candids
    
    jds = sorted([a['candidate']['jd'] for a in alerts])
    sec = 1/(24*3600.)
    reco_jds = [a['candidate']['jd'] for a in db.get_alerts_in_time_range(min(jds)-sec, max(jds)+sec)]
    assert reco_jds == jds
    
    reco_candids = [a['candid'] for a in db.get_alerts_in_cone(alerts[0]['candidate']['ra'], alerts[0]['candidate']['dec'], (2*u.deg).to(u.deg).value)]
    assert alerts[0]['candid'] in reco_candids
    
    # end the transaction to commit changes to the stats tables
    db._connection.execute('end')
    db._connection.execute('vacuum full')
    for table, stats in db.get_statistics().items():
        assert stats['rows'] >= db._connection.execute(db._meta.tables[table].count()).fetchone()[0]

def test_insert_future_schema(alert_generator, postgres):
    db = archive.ArchiveDB(postgres)

    alert, schema = next(alert_generator(True))
    schema['version'] = str(float(schema['version'])+10)
    with pytest.raises(ValueError) as e_info:
        db.insert_alert(alert, schema, 0, 0)

