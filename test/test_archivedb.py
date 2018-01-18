
import pytest
import fastavro
from glob import glob
import os
import time
from math import isnan

from ampel import archive

from sqlalchemy import select
from sqlalchemy.sql.functions import count
from collections import Iterable

@pytest.fixture
def alert_schema():
    parent = os.path.dirname(os.path.realpath(__file__)) + '/../'
    with open(parent+'alerts/ipac/121540793515135000.avro', 'rb') as f:
        reader = fastavro.reader(f)
        return reader.schema

@pytest.fixture(scope="function")
def mock_database(alert_schema):
    meta = archive.create_metadata(alert_schema)
    engine = archive.create_database(meta, 'sqlite:///:memory:')
    connection = engine.connect()
    return meta, connection

def alerts():
    """
    Generate alerts, filtering out anonymous photopoints (entries in
    prv_candidates with no candid)
    """
    parent = os.path.dirname(os.path.realpath(__file__)) + '/../'
    for fname in glob(parent+'alerts/ipac/*.avro'):
        with open(fname, 'rb') as f:
            for alert in fastavro.reader(f):
                alert['prv_candidates'] = [c for c in alert['prv_candidates'] if c['candid'] is not None]
                del alert['cutoutDifference']
                del alert['cutoutScience']
                del alert['cutoutTemplate']
                yield alert

def test_create_database(alert_schema):
    meta = archive.create_metadata(alert_schema)
    engine = archive.create_database(meta, 'sqlite:///:memory:', echo=True)
    connection = engine.connect()
    assert connection.execute('SELECT COUNT(*) from alert').first()[0] == 0

def test_insert_unique_alerts(mock_database):
    processor_id = 0
    meta, connection = mock_database
    timestamps = []
    for alert in alerts():
        timestamps.append(int(time.time()*1e6))
        archive.insert_alert(connection, meta, alert, processor_id, timestamps[-1])
    rows = connection.execute(select([meta.tables['alert'].c.ingestion_time])).fetchall()
    db_timestamps = sorted([tup[0] for tup in rows])
    assert timestamps == db_timestamps

def test_insert_duplicate_alerts(mock_database):
    processor_id = 0
    meta, connection = mock_database
    
    alert = next(alerts())
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['photopoint'])).first()[0] == len(alert['prv_candidates'])+1
    
    # inserting the same alert a second time does nothing
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['photopoint'])).first()[0] == len(alert['prv_candidates'])+1

def test_insert_duplicate_photopoints(mock_database):
    processor_id = 0
    meta, connection = mock_database
    
    alert = next(alerts())
    
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 1
    assert connection.execute(count(meta.tables['photopoint'])).first()[0] == len(alert['prv_candidates'])+1
    assert connection.execute(count(meta.tables['alert_photopoint_pivot'])).first()[0] == len(alert['prv_candidates'])+1
    
    # insert a new alert, containing the same photopoints. only the alert and pivot tables should gain entries
    alert['candid'] += 1
    archive.insert_alert(connection, meta, alert, processor_id, int(time.time()*1e6))
    assert connection.execute(count(meta.tables['alert'].columns.candid)).first()[0] == 2
    assert connection.execute(count(meta.tables['photopoint'])).first()[0] == len(alert['prv_candidates'])+1
    assert connection.execute(count(meta.tables['alert_photopoint_pivot'])).first()[0] == 2*(len(alert['prv_candidates'])+1)

def test_get_alert(mock_database):
    processor_id = 0
    meta, connection = mock_database
    
    timestamps = []
    for alert in alerts():
        timestamps.append(int(time.time()*1e6))
        archive.insert_alert(connection, meta, alert, processor_id, timestamps[-1])
    
    for alert in alerts():
        reco_alert = archive.get_alert(connection, meta, alert['candid'])
        # some necessary normalization on the alert
        for k,v in alert['candidate'].items():
            if isinstance(v, float) and isnan(v):
                alert['candidate'][k] = None
        assert alert == reco_alert