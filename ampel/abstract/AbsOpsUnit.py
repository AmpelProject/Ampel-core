from typing import Dict, Any, Optional

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
    def run(self, beacon: Optional[Dict[str,Any]]=None) -> Optional[Dict[str,Any]]:
        """
        :param beacon: the result of the previous run
        :returns:
          a BSON-serializable document summarizing the run. This will be
          supplied to the next invocation as `beacon`.
        """
        ...
