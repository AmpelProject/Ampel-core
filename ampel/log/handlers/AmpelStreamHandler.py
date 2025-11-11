#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/handlers/AmpelStreamHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.10.2018
# Last Modified Date:  06.11.2025
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, sys
from time import strftime
from typing import Literal

from rich.text import Text
from rich.console import Console

from ampel.log.LightLogRecord import LightLogRecord
from ampel.log.LogFlag import LogFlag
from ampel.util.collections import try_reduce
from ampel.util.mappings import compare_dict_values

levels = {1: 'DEBUG', 2: 'VERBOSE', 4: 'INFO', 8: 'SHOUT', 16: 'WARNING', 32: 'ERROR'}

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

	__slots__ = "__dict__", "print", "prev_record", "timefunc", \
		"prefix", "provenance", "nl", "aggregate_interval", \
		"datefmt", "timefmt", "cdate", "ctime", "cfile", "cline", "clvl"

	def __init__(self,
		std_stream: Literal['stdout', 'stderr'] = 'stderr',
		datefmt: str = "%Y-%m-%d ",
		timefmt: str = "%H:%M:%S ",
		level: int = LogFlag.INFO,
		aggregate_interval: float = 1.,
		density: Literal["default", "compact", "compacter", "headerless"] = "default",
		terminator: str = '\n',
		log_sep: str = '\n', # separator between aggregated log entries
		prefix: None | str = None,
		provenance: bool = True,
		color: bool = True,
		cdate: str = "cornflower_blue",    # rich color for date
		ctime: str = "light_sea_green",    # rich color for time
		# Note: dark_blue works too but salmon1 fits both light and dark backgrounds
		cfile: str = "salmon1",            # rich color for python file name
		cline: str = "rosy_brown",         # rich color for line number
		clvl: str = "grey53"               # rich color for log level
	) -> None:

		self.aggregate_interval = aggregate_interval
		self.dummy_record = LightLogRecord(name='dummy', levelno=1<<10, msg=None)
		self.prev_record: LightLogRecord = self.dummy_record
		if "%H" in datefmt:
			raise ValueError("Unexpected date format, please check/rebuild your config")
		self.datefmt = datefmt
		self.timefmt = timefmt
		self.prefix = prefix
		self.level = level
		self.nl = terminator
		self.log_sep = log_sep
		self.provenance = provenance
		self.ctime = ctime
		self.cdate = cdate
		self.cfile = cfile
		self.cline = cline
		self.clvl = clvl

		# Check env var NOCOLOR/NO_COLOR (case-insensitive, any non-empty value disables color)
		disable_color = (
			bool(os.environ.get("NOCOLOR", "").strip()) or
			bool(os.environ.get("NO_COLOR", "").strip())
		)

		# Disable colors when explicitly requested, when NOCOLOR is set or when output is redirected
		if not color or disable_color or not sys.stdout.isatty():
			self.console = Console(file=getattr(sys, std_stream), force_terminal=True, color_system=None)
		else:
			self.console = Console(file=getattr(sys, std_stream))

		self.print = self.console.print

		if density == "default":
			self.fields_check = ['extra', 'stock', 'channel', 'filename', 'levelno']
		elif density == "compact":
			self.fields_check = ['extra', 'stock', 'channel', 'levelno']
		elif density == "compacter":
			self.log_sep = ''
			self.fields_check = ['extra', 'stock', 'channel']
			self.handle = self.handle_compacter # type: ignore[method-assign]
		elif density == "headerless":
			self.handle = self.handle_headerless # type: ignore[method-assign]


	def handle(self, rec: LightLogRecord) -> None:

		prev_rec = self.prev_record

		# Same flag, date (+- 1 sec), tran_id and chans
		if not (
			rec.msg and rec.created - prev_rec.created < self.aggregate_interval and
			compare_dict_values(rec.__dict__, prev_rec.__dict__, self.fields_check)
		):
			self.print("", end=self.log_sep)
			self.print(self.format(rec))
			self.prev_record = rec

		if isinstance(rec.msg, str):
			self.print(" " + rec.msg.replace("\n", "\n "))


	def handle_compacter(self, rec: LightLogRecord) -> None:

		prev_rec = self.prev_record

		# Same flag, date (+- 1 sec), tran_id and chans
		if not (
			rec.msg and rec.created - prev_rec.created < self.aggregate_interval and
			compare_dict_values(rec.__dict__, prev_rec.__dict__, self.fields_check)
		):
			self.print("", end=self.log_sep)
			self.print(self.format(rec))
			self.prev_record = rec

		if isinstance(rec.msg, str):
			self.print(" " + rec.msg.replace("\n", "\n "))


	def handle_headerless(self, rec: LightLogRecord) -> None:
		if rec.msg:
			self.print(rec.msg)
		if rec.extra:
			if rec.msg:
				self.print(" ")
			self.print(str(rec.extra))


	def format(self, record: LightLogRecord) -> Text:

		t = Text()
		t.append(strftime(self.datefmt), style=self.cdate)
		t.append(strftime(self.timefmt), style=self.ctime)

		if self.prefix:
			t.append(self.prefix + " ")

		# Location (deactivated, takes space, no real benefit)
		"""
		lvl = record.levelno
		if lvl & 128:
			t.append(' UNIT')
		if lvl & 256:
			t.append(' CORE')
		"""

		if self.provenance and record.filename:
			if record.filename[0] == '<': # ipython
				t.append(record.filename, style=self.cfile)
			else:
				t.append(record.filename[:-3], style=self.cfile)
				t.append(":")
				t.append(str(record.lineno), style=self.cline)

		t.append(" " + levels[record.levelno >> 9], style=self.clvl)

		if record.extra:
			suffix = [f'{k}={record.extra[k]}' for k in record.extra]
		else:
			suffix = []

		if record.stock:
			suffix.append(f's={record.stock}') # type: ignore[str-bytes-safe]

		if record.channel:
			suffix.append(f'c={try_reduce(record.channel)}')

		if getattr(record, 'unit', None):
			suffix.append(f'unit={record.unit}')

		if suffix:
			t.append(f" [{', '.join(suffix)}]")

		return t


	def flush(self) -> None:
		self.console.file.flush()


	def break_aggregation(self) -> None:
		self.prev_record = self.dummy_record
