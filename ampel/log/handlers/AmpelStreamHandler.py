#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/handlers/AmpelStreamHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.10.2018
# Last Modified Date:  25.01.2026
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, sys
from time import strftime, localtime
from typing import Literal, Any
from multiprocessing import current_process

from rich.text import Text
from rich.console import Console
from rich.highlighter import ReprHighlighter

from ampel.log.LightLogRecord import LightLogRecord
from ampel.log.LogFlag import LogFlag
from ampel.util.collections import try_reduce
from ampel.util.mappings import compare_dict_values

levels = {1: 'Debug', 2: 'Verbose', 4: 'Info', 8: 'SHOUT', 16: 'Warning', 32: 'ERROR'}

class AmpelStreamHandler:
	"""
	:param int aggregate_interval: logs with similar attributes (log level, ...) are aggregated.
	*aggregate_interval* is the max interval of time in seconds during which log aggregation occurs.
	Note that it impacts the logging time granularity.
	:param density: Controls how aggressively log entries are grouped.
	- "default": Aggregates entries only when log level, originating class,
	  and 'extra' fields match. Any difference breaks aggregation.
	- "compact": Aggregates entries even when log level or originating class
	  differ, as long as 'extra' fields match. Produces denser output.
	- "inline": Produces a single-line disables aggregation entirely (console only)
	"""

	level: int

	__slots__ = "__dict__", "print", "prev_record", "level", "log_sep", "fields_check", \
		"prefix", "provenance", "nl", "aggregate_interval", "datefmt", "timefmt", \
		"cdate", "ctime", "cfile", "cline", "clvl", "std_stream", "density", "color"

	def __init__(self,
		std_stream: Literal['stdout', 'stderr'] = 'stderr',
		datefmt: str = "%Y-%m-%d ",
		timefmt: str = "%H:%M:%S ",
		level: int = LogFlag.INFO,
		aggregate_interval: float = 1.,
		density: Literal["default", "compact", "inline"] = "default",
		terminator: str = '\n',
		log_sep: str = '\n ', # separator between aggregated log entries
		prefix: None | str = None,
		provenance: bool = True,
		color: bool = True,
		extra: None | dict = None,
		cdate: str = "cornflower_blue",    # rich color for date
		ctime: str = "light_sea_green",    # rich color for time
		# Note: dark_blue works too but salmon1 fits both light and dark backgrounds
		cfile: str = "salmon1",            # rich color for python file name
		cline: str = "rosy_brown",         # rich color for line number
		clvl: str = "grey53"               # rich color for log level
	) -> None:

		self.aggregate_interval = aggregate_interval
		if "%H" in datefmt:
			raise ValueError("Unexpected date format, please check/rebuild your config")
		self.datefmt = datefmt
		self.timefmt = timefmt
		self.prefix = prefix
		if isinstance(level, str):
			self.level = int(getattr(LogFlag, level.upper()))
		else:
			self.level = level
		self.nl = terminator
		self.log_sep = log_sep
		self.provenance = provenance
		self.ctime = ctime
		self.cdate = cdate
		self.cfile = cfile
		self.cline = cline
		self.clvl = clvl
		self.std_stream = std_stream
		self.density = density
		self.color = color
		self.extra = extra

		if density == "default":
			self.fields_check = ['extra', 'stock', 'channel', 'filename', 'levelno']
		elif density == "compact":
			self.fields_check = ['extra', 'stock', 'channel', 'levelno']
		elif density == "inline":
			self.aggregate_interval = 0

		self.setup()


	def setup(self):

		# Rebuild dummy and prev record
		self.dummy_record = LightLogRecord(name='dummy', levelno=1<<12, msg=None)
		self.prev_record = self.dummy_record

		# Check env var NOCOLOR/NO_COLOR
		disable_color = (
			bool(os.environ.get("NOCOLOR", "").strip()) or
			bool(os.environ.get("NO_COLOR", "").strip())
		)

		stream = getattr(sys, self.std_stream)

		# Rebuild console
		if not self.color or disable_color or not stream.isatty():
			self.console = Console(file=stream, force_terminal=True, color_system=None)
		else:
			self.console = Console(file=stream)

		self.print = self.console.print
		self.hl = ReprHighlighter()


	def __getstate__(self):
		"""
		Strip runtime-only objects (Console, print, dummy_record, prev_record)
		and return only picklable configuration data.
		"""
		return {
			'aggregate_interval': self.aggregate_interval,
			'datefmt': self.datefmt,
			'timefmt': self.timefmt,
			'prefix': self.prefix,
			'level': self.level,
			'nl': self.nl,
			'log_sep': self.log_sep,
			'provenance': self.provenance,
			'ctime': self.ctime,
			'cdate': self.cdate,
			'cfile': self.cfile,
			'cline': self.cline,
			'clvl': self.clvl,
			'density': self.density,
			'color': self.color,
			'std_stream': self.std_stream,
			'fields_check': getattr(self, 'fields_check', None),
			'extra': self.extra
		}


	def __setstate__(self, state):
		""" Rebuild the handler in the worker process. Console and print are reconstructed here. """
		for k, v in state.items():
			setattr(self, k, v)
		if self.extra:
			self.extra |= {"process": current_process().name}
		else:
			self.extra =  {"process": current_process().name}
		self.setup()


	def handle(self, rec: LightLogRecord) -> None:

		if rec.levelno < self.level:
			return

		if self.extra:
			if rec.extra:
				rec.extra |= self.extra
			else:
				rec.extra = self.extra

		out = Text()

		# Log interval > 1 sec, different 'extra', 'stock', 'channel', 'levelno', 'filename' if not compact
		if not ( # Start a new block
			rec.msg and (rec.created - self.prev_record.created) < self.aggregate_interval and
			compare_dict_values(rec.__dict__, self.prev_record.__dict__, self.fields_check)
		):
			if self.density == "default":
				out.append(self.nl)
			out.append(self.format(rec))
			if self.density != "inline":
				out.append(self.nl)
			self.prev_record = rec

		if isinstance(rec.msg, str):
			self.print(out + self.hl(" " + rec.msg.replace("\n", self.log_sep)))

		elif out.plain:
			self.print(out)


	def format(self, record: LightLogRecord) -> Text:

		t = Text()
		created = localtime(record.created)
		t.append(strftime(self.datefmt, created), style=self.cdate)
		t.append(strftime(self.timefmt, created), style=self.ctime)

		if self.prefix:
			t.append(self.prefix + " ")

		# Location
		lvl = record.levelno
		if lvl & 128:
			t.append('Unit ', style='grey63')
		if lvl & 256:
			t.append('Core ', style='grey63')

		if self.provenance and record.filename:
			if record.filename[0] == '<': # ipython
				t.append(record.filename + " ", style=self.cfile)
			else:
				t.append(record.filename[:-3], style=self.cfile)
				t.append(":")
				t.append(str(record.lineno) + " ", style=self.cline)

		t.append(levels[record.levelno >> 11] + " ", style=self.clvl)

		self.format_extra(t, record)
		return t


	def format_extra(self, t: Text, record: LightLogRecord):

		d: dict[str, Any] = {}

		if record.stock:
			d["s"] = record.stock

		if record.channel:
			d["c"] = try_reduce(record.channel)

		if getattr(record, 'unit', None):
			d["unit"] = record.unit

		if record.extra:
			d |= record.extra

		if d:
			t.append("[")
			for k, v in d.items():
				self.append_extra(t, k, v)
			t.append("]")


	def append_extra(self, t: Text, k, v) -> None:
		t.append(k, style=self.ctime)
		t.append("=", style='grey63')
		t.append(str(v), style=self.cdate)


	def flush(self) -> None:
		self.console.file.flush()


	def break_aggregation(self) -> None:
		self.prev_record = self.dummy_record
