
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
    admin, config_db = create_databases(os.environ['MONGO'], db_name, configs)
    user, password = 'testy', 'testington'
    specs = config_db['global'].find_one({'_id':'dbSpecs'})['databases']
    databases = [config_db.name] + [d['dbName'] for d in specs.values()]
    roles = [{'db':name, "role": "readWrite"} for name in databases]
    admin.add_user(user, password, roles=roles)

    uri = "mongodb://{}:{}@{}/".format(user,password,os.environ['MONGO'])
    yield uri, db_name
    for db in [admin.client.get_database(role['db']) for role in roles]:
        db.command("dropDatabase")

def test_instantiate_alertprocessor(alert_generator, test_database, caplog):
    """Can an AlertProcessor be instantiated cleanly?"""
    uri, config_db_name = test_database
    ap = AlertProcessor(db_host=uri, config_db=config_db_name)
    spec = ap.global_config['dbSpecs']
    print(spec)
    def get_collection(alertprocessor, name):
        spec = alertprocessor.global_config['dbSpecs']['databases'][name]
        return alertprocessor.mongo_client[spec['dbName']][spec['collectionName']]
    transients = get_collection(ap, 'transients')
    logs = get_collection(ap, 'jobs')
    assert transients.find({}).count() == 0
    
    # ensure that the RandomFilter always does the same thing
    random.seed('reproducibility considered good')
    
    #assert ap.run(islice(alert_generator(), 100)) == 100
    # FIXME: remove once upper limit support is in
    def clean_alerts():
        for alert in alert_generator():
            alert['prv_candidates'] = [v for v in alert['prv_candidates'] if v['candid'] is not None]
            yield alert
    assert ap.run(islice(clean_alerts(), 100)) == 100
    
    # ensure that all logs ended up in the db
    assert logs.find({}).count() == 1
    record = next(logs.find({}))
    assert len(record["records"]) == len(caplog.records)
    print(len(caplog.records))
    
    assert transients.find({}).count() == 784

