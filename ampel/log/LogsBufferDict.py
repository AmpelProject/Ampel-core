#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/log/LogsBufferDict.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 02.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys
from typing import List, Dict, Any
if sys.version_info.minor > 8:
	from typing import TypedDict
else:
	from typing_extensions import TypedDict


class LogsBufferDict(TypedDict, total=False):
	"""
	Allows to concatenate various log entries with different 'extra' parameters
	(that in this case normally do not self-aggregate automatically).
	This class is used for example by the AlertProcessor to ensure the aggregation
	of log entries emitted by different ingesters (T0, T1, T2)
	"""
	logs: List[str]
	extra: Dict[str, Any]
	err: bool
