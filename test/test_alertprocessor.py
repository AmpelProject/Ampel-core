
from ampel.pipeline.t0.AlertProcessor import AlertProcessor, create_databases
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.archive import ArchiveDB

import os
from glob import glob
import random
from itertools import islice

import pytest

@pytest.fixture
def test_database():
    pattern = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/../config/test/*/*.json')
    configs = glob(pattern)
    db_name = 'ampel-test-config'
    dbs = create_databases(os.environ['MONGO'], db_name, configs)
    admin, config_db = dbs[:2]
    user, password = 'testy', 'testington'
    admin.add_user(user, password, roles=[{"db" : db.name, "role": "readWrite"} for db in dbs[1:]])

    uri = "mongodb://{}:{}@{}/".format(user,password,os.environ['MONGO'])
    yield uri, db_name
    for db in dbs[1:]:
        db.command("dropDatabase")

def test_instantiate_alertprocessor(alert_generator, test_database, caplog):
    """Can an AlertProcessor be instantiated cleanly?"""
    uri, config_db_name = test_database
    ap = AlertProcessor(db_host=uri, input_db=config_db_name)
    spec = ap.global_config['dbSpecs']
    transients = ap.mongo_client[spec['transients']['dbName']][spec['transients']['collectionName']]
    logs = ap.mongo_client[spec['logs']['dbName']][spec['logs']['collectionName']]
    assert transients.find({}).count() == 0
    
    # ensure that the RandomFilter always does the same thing
    random.seed('reproducibility considered good')
    
    ap.run(islice(alert_generator(), 100))
    
    # ensure that all logs ended up in the db
    assert logs.find({}).count() == 1
    record = next(logs.find({}))
    assert len(record["records"]) == len(caplog.records)
    
    assert transients.find({}).count() == 810

def test_alertprocessor_stream(alert_stream, caplog):
    
    config = os.path.dirname(os.path.realpath(__file__)) + '/../mockdb/config.json'
    ap = AlertProcessor(mock_db=True, config_file=config)
    
    archive = ArchiveDB("sqlite:///:memory:")
    loader = ZIAlertLoader(archive, "localhost:9092", b'alerts')
    
    ap.run(loader)
    
    assert ap.mongo_client['Ampel']['main'].find({}).count() == 318
