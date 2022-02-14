#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsOpsUnit.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: Unspecified
# Last Modified By  : jvs

from typing import Dict, Any, Optional
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit
from ampel.log.AmpelLogger import AmpelLogger
from ampel.types import Traceless

class AbsOpsUnit(AmpelABC, ContextUnit, abstract=True):
    """
    A unit for performing scheduled maintenance tasks not associated with a
    particular processing tier: collecting metrics, reporting exceptions, etc.
    """

    logger: Traceless[AmpelLogger]

    @abstractmethod
    def run(self, beacon: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        :param beacon: the result of the previous run
        :returns:
          a BSON-serializable document summarizing the run. This will be
          supplied to the next invocation as `beacon`.
        """
        ...
