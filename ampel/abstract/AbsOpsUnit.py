from typing import Any

from ampel.core.AdminUnit import AdminUnit
from ampel.base import abstractmethod
from ampel.log.AmpelLogger import AmpelLogger


class AbsOpsUnit(AdminUnit, abstract=True):
    """
    A unit for performing sceduled maintenance tasks not associated with a
    particular processing tier: collecting metrics, reporting exceptions, etc.
    """

    logger: AmpelLogger

    @abstractmethod
    def run(self) -> Any:
        ...
