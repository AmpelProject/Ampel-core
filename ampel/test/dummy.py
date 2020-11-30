import time

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit


class Sleepy(AbsProcessorUnit):
    """
    A processor that does nothing (especially not touching the db, which is not
    mocked in subprocesses)
    """

    def run(self):
        time.sleep(1)
