#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/data/GlobalInfo.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.10.2018
# Last Modified Date: 14.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic.dataclasses import dataclass
from datetime import datetime

@dataclass
class GlobalInfo:
	event: str
	lastRun: datetime
	processedAlerts: int = None
	adminMsg: str = None
