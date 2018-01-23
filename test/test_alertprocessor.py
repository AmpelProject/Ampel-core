
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader

import os
import random

def test_instantiate_alertprocessor(alert_generator, caplog):
    """Can an AlertProcessor be instantiated cleanly?"""
    config = os.path.dirname(os.path.realpath(__file__)) + '/../mockdb/config.json'
    ap = AlertProcessor(mock_db=True, config_file=config)
    assert ap.mongo_client['Ampel']['main'].find({}).count() == 0
    
    # ensure that the RandomFilter always does the same thing
    random.seed('reproducibility considered good')
    
    ap.run(alert_generator())
    
    # ensure that all logs ended up in the db
    assert ap.mongo_client['events']['jobs'].find({}).count() == 1
    record = next(ap.mongo_client['events']['jobs'].find({}))
    assert len(record["records"]) == len(caplog.records)
    
    assert ap.mongo_client['Ampel']['main'].find({}).count() == 318

def test_alertprocessor_stream(alert_stream, caplog):
    
    config = os.path.dirname(os.path.realpath(__file__)) + '/../mockdb/config.json'
    ap = AlertProcessor(mock_db=True, config_file=config)
    
    loader = ZIAlertLoader("localhost:9092", b'alerts')
    
    ap.run(loader)
    
    assert ap.mongo_client['Ampel']['main'].find({}).count() == 318
