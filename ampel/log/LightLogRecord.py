#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/logging/LightLogRecord.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                22.04.2020
# Last Modified Date:  22.04.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from typing import Any
from ampel.types import ChannelId, StockId

class LightLogRecord:
	"""
	LogRecord class similar to the one provided by standard 'logging' module but faster.

	In []: %timeit LogRecord(name=12, pathname=None, level=12, lineno=12, exc_info=None, msg=None, args=None)
	Out[]: 4.7 µs ± 77.9 ns per loop (mean ± std. dev. of 7 runs, 100000 loops each)

	In []: %timeit LightLogRecord(name=12, levelno=1, msg=None)
	Out[]: 657 ns ± 6.51 ns per loop (mean ± std. dev. of 7 runs, 1000000 loops each)
	"""

	name: int | str
	levelno: int
	msg: None | str | dict[str, Any]
	channel: None | ChannelId | list[ChannelId]
	stock: None | StockId
	extra: None | dict[str, Any]

	def __init__(self,
		name: int | str,
		levelno: int,
		msg: None | str | dict[str, Any] = None
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
