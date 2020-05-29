#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handlers/AmpelStreamHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.10.2018
# Last Modified Date: 10.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, time
from sys import _getframe
from os.path import basename
from typing import Literal, Dict, List, Optional
from ampel.log.LogRecordFlag import LogRecordFlag
from ampel.log.LighterLogRecord import LighterLogRecord
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.AmpelUnit import AmpelUnit
from ampel.util.mappings import compare_dict_values

mappings = {'a': 'alert', 'n': 'new'}
levels = {1: 'DEBUG', 2: 'VERBOSE', 4: 'INFO', 8: 'SHOUT', 16: 'WARNING', 32: 'ERROR'}

# flake8: noqa: E101
class AmpelStreamHandler(AmpelUnit):
	"""
	:param int aggregate_interval: logs with similar attributes (log level, ...) are aggregated
	in one document instead of being split into several documents (spares some index RAM).
	*aggregate_interval* is the max interval of time in seconds during which log aggregation occurs.
	Beyond this value, a new log document is created no matter what.
	Note that it impacts the logging time granularity.

	:param density: whether log header (including timestamp) should be displayed on the console:

	- "default" (aggreation break if log-level, class-origin or 'extra' differ between log entries):
	2020-03-03 03:01:23 DistConfigBuilder VERBOSE
	 Loading conf/ampel-contrib-hu/unit.conf from distribution 'ampel-contrib-hu'

	2020-03-03 03:01:24 UnitConfigCollector VERBOSE
	 Adding T0 base unit: DecentFilter
	 Adding T0 base unit: SimpleDecentFilter
	 Adding T0 base unit: XShooterFilter

	- "compact" (aggregates log entries even if log entries originates from different classes):
	2020-03-03 03:01:23 DistConfigBuilder VERBOSE
	 Loading conf/ampel-contrib-hu/unit.conf from distribution 'ampel-contrib-hu'
	 Adding T0 base unit: DecentFilter
	 Adding T0 base unit: SimpleDecentFilter
	 Adding T0 base unit: XShooterFilter

	- "headerless" (Skip the log headers completely (no timestamp, no log level...):
	Loading conf/ampel-contrib-hu/unit.conf from distribution 'ampel-contrib-hu'
	Adding T0 base unit: DecentFilter
	Adding T0 base unit: SimpleDecentFilter
	Adding T0 base unit: XShooterFilter
	"""

	std_stream: Literal['stdout', 'stderr'] = 'stdout'
	datefmt: str = "%Y-%m-%d %H:%M:%S"
	level: int = LogRecordFlag.SHOUT
	aggregate_interval: float = 1.
	density: Literal["default", "compact", "compacter", "headerless"] = "default"
	terminator: str = '\n'
	log_sep: str = '\n' # separator between aggregated log entries
	prefix: Optional[str] = None
	provenance: bool = True


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs) # type: ignore[call-arg]
		self.dummy_record = LighterLogRecord(name='dummy', levelno=1<<10, msg=None)
		self.prev_record: LighterLogRecord = self.dummy_record
		self.nl = self.terminator
		self.nlsp = self.nl + ' ' # newline space
		self.stream = getattr(sys, self.std_stream)
		self.timefunc = time.strftime

		if self.density == "default":
			self.fields_check = ['extra', 'stock', 'channel', 'filename', 'levelno']
		elif self.density == "compact":
			self.fields_check = ['extra', 'stock', 'channel', 'levelno']
		elif self.density == "compacter":
			self.log_sep = ''
			self.fields_check = ['extra', 'stock', 'channel']
			self.handle = getattr(self, f"handle_compacter") # type: ignore


	def handle(self, rec: LighterLogRecord) -> None:

		prev_rec = self.prev_record

		# Same flag, date (+- 1 sec), tran_id and chans
		if (
			rec.msg and rec.created - prev_rec.created < self.aggregate_interval and
			compare_dict_values(rec.__dict__, prev_rec.__dict__, self.fields_check) # type: ignore[arg-type]
		):
			self.stream.write(f' {rec.msg}{self.nl}')
		else:
			self.stream.write(f'{self.log_sep}{self.format(rec)}{self.nl}')
			self.prev_record = rec


	def handle_compacter(self, rec: LighterLogRecord) -> None:

		prev_rec = self.prev_record

		# Same flag, date (+- 1 sec), tran_id and chans
		if (
			rec.msg and rec.created - prev_rec.created < self.aggregate_interval and
			compare_dict_values(rec.__dict__, prev_rec.__dict__, self.fields_check) # type: ignore[arg-type]
		):
			self.stream.write(f" {rec.msg}{self.nl}")
		else:
			self.stream.write(f"{self.log_sep}{self.format(rec)}{self.nl}")
			self.prev_record = rec


	def handle_headerless(self, rec: LighterLogRecord) -> None:
		if rec.msg == None:
			return
		self.stream.write(rec.msg)
		self.stream.write(self.terminator)


	def format(self, record: LighterLogRecord) -> str:

		out = self.timefunc(self.datefmt)
		lvl = record.levelno

		if self.prefix:
			out += self.prefix

		# Location
		if lvl & 64:
			out += ' UNIT'
		if lvl & 128:
			out += ' CORE'

		if self.provenance:
			if record.filename[0] == '<': # ipython
				out += f' {record.filename} {levels[lvl >> 8]}'
			else:
				out += f' {record.filename[:-3]}:{record.lineno} {levels[lvl >> 8]}'
		else:
			out += f' {levels[lvl >> 8]}'

		# Note: we do not print infos regarding tier or run schedule

		if record.extra:
			suffix = [f'{mappings[k] if k in mappings else k}={record.extra[k]}' for k in record.extra]
		else:
			suffix = []

		if record.channel:
			suffix.insert(0, f'channel={record.channel}')

		if record.stock:
			suffix.insert(0, f'stock={record.stock}') # type: ignore

		if suffix:
			out += f' [{", ".join(suffix)}]'

		if isinstance(record.msg, str):
			return out + "\n " + record.msg.replace("\n", "\n ") # type: ignore[union-attr]

		return out
