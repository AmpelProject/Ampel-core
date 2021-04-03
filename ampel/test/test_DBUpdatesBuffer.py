from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from pymongo import UpdateOne, InsertOne
from contextlib import contextmanager


def test_metrics(dev_context, ampel_logger):
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=0, logger=ampel_logger)
    get_sample_value = AmpelMetricsRegistry.registry().get_sample_value
    cols = ["stock", "t0", "t1", "t2"]

    before = {k: get_sample_value("ampel_db_ops_total", {"col": k}) or 0 for k in cols}
    updates_buffer.add_t0_update(InsertOne({"_id": 0, "foo": "bar"}))
    updates_buffer.push_updates()
    after = {k: get_sample_value("ampel_db_ops_total", {"col": k}) or 0 for k in cols}

    for k in cols:
        if k == "t0":
            assert after[k] - before[k] == 1, f"ops count was incremented for {k}"
        else:
            assert after[k] - before[k] == 0, f"ops count was not incremented for {k}"

    before = {k: get_sample_value("ampel_db_errors_total", {"col": k}) or 0 for k in cols}
    updates_buffer.add_t0_update(InsertOne({"_id": 0, "foo": "bar"}))
    updates_buffer.push_updates()
    after = {k: get_sample_value("ampel_db_errors_total", {"col": k}) or 0 for k in cols}

    for k in cols:
        if k == "t0":
            assert after[k] - before[k] == 1, f"error count was incremented for {k}"
        else:
            assert after[k] - before[k] == 0, f"error count was not incremented for {k}"
