#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/LogsBufferDict.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 10.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import TypedDict, List, Dict, Any

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
