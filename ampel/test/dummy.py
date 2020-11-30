import time

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit


class Sleepy(AbsProcessorUnit):
    """
    A processor that does nothing (especially not touching the db, which is not
    mocked in subprocesses)
    """

    def run(self):
        time.sleep(1)


class CaptainObvious(AbsStockT2Unit):
    def run(self, stock_record):
        return {"id": stock_record["_id"]}
