#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/log/handlers/AmpelStreamHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.10.2018
# Last Modified Date: 24.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, time
from sys import _getframe
from os.path import basename
from time import strftime
from typing import Literal, Dict, List, Optional
from ampel.log.LogFlag import LogFlag
from ampel.log.LightLogRecord import LightLogRecord
from ampel.util.mappings import compare_dict_values
from ampel.util.collections import try_reduce

levels = {1: 'DEBUG', 2: 'VERBOSE', 4: 'INFO', 8: 'SHOUT', 16: 'WARNING', 32: 'ERROR'}

# flake8: noqa: E101
class AmpelStreamHandler:
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

	__slots__ = "__dict__", "stream", "prev_record", "timefunc", \
		"prefix", "provenance", "nl", "aggregate_interval"


	def __init__(self,
		std_stream: Literal['stdout', 'stderr'] = 'stderr',
		datefmt: str = "%Y-%m-%d %H:%M:%S",
		level: int = LogFlag.INFO,
		aggregate_interval: float = 1.,
		density: Literal["default", "compact", "compacter", "headerless"] = "default",
		terminator: str = '\n',
		log_sep: str = '\n', # separator between aggregated log entries
		prefix: Optional[str] = None,
		provenance: bool = True
	) -> None:

		self.aggregate_interval = aggregate_interval
		self.dummy_record = LightLogRecord(name='dummy', levelno=1<<10, msg=None)
		self.prev_record: LightLogRecord = self.dummy_record
		self.datefmt = datefmt
		self.prefix = prefix
		self.level = level
		self.nl = terminator
		self.log_sep = log_sep
		self.provenance = provenance
		self.stream = getattr(sys, std_stream)

		if density == "default":
			self.fields_check = ['extra', 'stock', 'channel', 'filename', 'levelno']
		elif density == "compact":
			self.fields_check = ['extra', 'stock', 'channel', 'levelno']
		elif density == "compacter":
			self.log_sep = ''
			self.fields_check = ['extra', 'stock', 'channel']
			self.handle = getattr(self, f"handle_compacter") # type: ignore[assignment]
		elif density == "headerless":
			self.handle = getattr(self, f"handle_headerless") # type: ignore[assignment]


	def handle(self, rec: LightLogRecord) -> None:

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


	def handle_compacter(self, rec: LightLogRecord) -> None:

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


	def handle_headerless(self, rec: LightLogRecord) -> None:
		if rec.msg:
			self.stream.write(rec.msg)
		if rec.extra:
			if rec.msg:
				self.stream.write(f" ")
			self.stream.write(str(rec.extra))
		self.stream.write(self.nl)


	def format(self, record: LightLogRecord) -> str:

		out = strftime(self.datefmt)
		lvl = record.levelno

		if self.prefix:
			out += self.prefix

		# Location
		if lvl & 64:
			out += ' UNIT'
		if lvl & 128:
			out += ' CORE'

		if self.provenance and record.filename:
			if record.filename[0] == '<': # ipython
				print(lvl)
				out += f' {record.filename} {levels[lvl >> 8]}'
			else:
				out += f' {record.filename[:-3]}:{record.lineno} {levels[lvl >> 8]}'
		else:
			out += f' {levels[lvl >> 8]}'

		if record.extra:
			suffix = [f'{k}={record.extra[k]}' for k in record.extra]
		else:
			suffix = []

		if record.stock:
			suffix.append(f's={record.stock}') # type: ignore

		if record.channel:
			suffix.append(f'c={try_reduce(record.channel)}')

		if suffix:
			out += f' [{", ".join(suffix)}]'

		if isinstance(record.msg, str):
			return out + "\n " + record.msg.replace("\n", "\n ") # type: ignore[union-attr]

		return out


	def flush(self) -> None:
		self.stream.flush()


	def break_aggregation(self) -> None:
		self.prev_record = self.dummy_record
