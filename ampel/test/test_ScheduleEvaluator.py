from contextlib import nullcontext

import pytest
import schedule as sched

from ampel.config.ScheduleEvaluator import ScheduleEvaluator


@pytest.mark.parametrize(
    ("line","unit","interval","error"),
    [
        ("every(30).seconds", "seconds", 30, None),
        ("every().day.at('16:00')", "days", 1, None),
        ("requests.get('http://malicio.us')", None, None, ValueError),
    ],
)
def test_evaluate(line, unit, interval, error):
    evaluator = ScheduleEvaluator()
    with pytest.raises(error) if error else nullcontext():
        job = evaluator(sched.Scheduler(), line)
        assert job.unit == unit
        assert job.interval == interval
