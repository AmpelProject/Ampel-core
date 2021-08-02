#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/LightLogRecord.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.04.2020
# Last Modified Date: 22.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Union, List, Any, Dict, Optional
from ampel.types import ChannelId, StockId

class LightLogRecord:
	"""
	LogRecord class similar to the one provided by standard 'logging' module but faster.

	In []: %timeit LogRecord(name=12, pathname=None, level=12, lineno=12, exc_info=None, msg=None, args=None)
	Out[]: 4.7 µs ± 77.9 ns per loop (mean ± std. dev. of 7 runs, 100000 loops each)

	In []: %timeit LightLogRecord(name=12, levelno=1, msg=None)
	Out[]: 657 ns ± 6.51 ns per loop (mean ± std. dev. of 7 runs, 1000000 loops each)
	"""

	name: Union[int, str]
	levelno: int
	msg: Optional[Union[str, Dict[str, Any]]]
	channel: Optional[Union[ChannelId, List[ChannelId]]]
	stock: Optional[StockId]
	extra: Optional[Dict[str, Any]]

	def __init__(self,
		name: Union[int, str],
		levelno: int,
		msg: Optional[Union[str, Dict[str, Any]]] = None
	) -> None:

		d = self.__dict__
		d['name'] = name
		d['created'] = time()
		d['levelno'] = levelno
		d['msg'] = msg

	def getMessage(self):
		return self.msg

	def __getattr__(self, k):
		return self.__dict__[k] if k in self.__dict__ else None
