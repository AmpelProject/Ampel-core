#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handler/LoggingHandlerProtocol.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.05.2020
# Last Modified Date: 09.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union
from typing_extensions import Protocol, runtime_checkable
from logging import LogRecord
from ampel.log.LighterLogRecord import LighterLogRecord


@runtime_checkable
class LoggingHandlerProtocol(Protocol):

	level: int

	def handle(self, record: Union[LighterLogRecord, LogRecord]) -> None:
		...

	def flush(self) -> None:
		...
