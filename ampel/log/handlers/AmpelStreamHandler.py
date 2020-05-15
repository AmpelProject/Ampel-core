#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/AmpelLoggingStreamHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.10.2018
# Last Modified Date: 05.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal, Optional
from logging import StreamHandler, DEBUG, _logRecordFactory # type: ignore
from ampel.logging.AmpelLogger import AmpelLogger


# flake8: noqa: E101
class AmpelLoggingStreamHandler(StreamHandler):


	def __init__(self,
		stream, level=DEBUG,
		aggregate_interval: float = 1.,
		log_separator = StreamHandler.terminator,
		options: Optional[Literal['default', 'compact', 'compacter', 'headerless']] = None
	) -> None:
		"""
		:param int aggregate_interval: logs with similar attributes (log level, ...)
		are aggregated in one document instead of being split
		into several documents (spares some index RAM). *aggregate_interval* is the max interval
		of time in seconds during which log aggregation takes place. Beyond this value,
		a new log document is created no matter what. It thus impacts the logging time granularity.

		:param options: whether log header (including timestamp) should be displayed on the console:

		- "default" (aggreation break if log-level, class-origin or extra differ between log entries):
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

		super().__init__(stream)
		self.aggregate_interval = aggregate_interval
		self.prev_record = _logRecordFactory(None, 0, None, None, None, None, None, None, None)
		self.prev_record.recs = []
		self.setLevel(level)
		self.nl = self.terminator
		self.logsep = log_separator
		self.nlsp = self.nl + ' ' # newline space
		if not options:
			options = AmpelLogger.console_logging_option
		self.emit = getattr(self, f"emit_{options}") # type: ignore


	def emit_default(self, record):

		extra = getattr(record, 'extra', None)
		prev_rec = self.prev_record

		# Same flag, date (+- 1 sec), tran_id and chans
		if (
			AmpelLogger.aggregation_ok and prev_rec and
			record.levelno == prev_rec.levelno and
			record.filename == prev_rec.filename and
			record.created - prev_rec.created < self.aggregate_interval and
			extra == getattr(prev_rec, 'extra') and record.msg
		):
			self.stream.write(f" {record.getMessage()}{self.nl}")
		else:
			self.stream.write(f"{self.logsep}{self.format(record)}{self.nl}")
			self.prev_record = record

		self.flush()


	def emit_compact(self, record):

		extra = getattr(record, 'extra', None)
		prev_rec = self.prev_record

		if (
			AmpelLogger.aggregation_ok and prev_rec and
			record.levelno == prev_rec.levelno and
			record.created - prev_rec.created < self.aggregate_interval and
			extra == getattr(prev_rec, 'extra') and record.msg
		):
			self.stream.write(f" {record.getMessage()}{self.nl}")
		else:
			self.stream.write(f"{self.logsep}{self.format(record)}{self.nl}")
			self.prev_record = record

		self.flush()


	def emit_compacter(self, rec):

		extra = getattr(rec, 'extra', None)
		prev_rec = self.prev_record

		if (
			AmpelLogger.aggregation_ok and prev_rec and
			rec.created - prev_rec.created < self.aggregate_interval and
			extra == getattr(prev_rec, 'extra', None) and rec.msg
		):
			prev_rec.recs.append(rec)
		else:

			first_rec = prev_rec.recs[0]
			first_rec.levelno = max([r.levelno for r in prev_rec.recs])
			if rec.levelno > first_rec.levelno:
				first_rec.levelno = rec.levelno
			self.stream.write( # Using fstring is faster than using multiple stream.write calls
				f"{self.logsep}{self.format(first_rec)}{self.nlsp}" +
				self.nlsp.join([el.getMessage() for el in prev_rec.recs[1:]]) +
				f"{self.nlsp}{rec.getMessage()}{self.nl}"
			)
			rec.recs = []
			self.prev_record = rec

		self.flush()


	def emit_headerless(self, record):
		self.stream.write(record.getMessage())
		self.stream.write(self.terminator)
		self.flush()
