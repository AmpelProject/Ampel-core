
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
import os

def test_instantiate_alertprocessor():
    """Can an AlertProcessor be instantiated cleanly?"""
    config = os.path.dirname(os.path.realpath(__file__)) + '/../mockdb/config.json'
    ap = AlertProcessor(mock_db=True, config_file=config)
    assert ap.mongo_client['Ampel']['main'].find({}).count() == 0
