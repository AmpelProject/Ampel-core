#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/var/DBLoggingHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.12.2017
# Last Modified Date:  29.03.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import struct, socket
from bson import ObjectId
from typing import TYPE_CHECKING
from logging import LogRecord
from pymongo.errors import BulkWriteError
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.util.mappings import compare_dict_values
from ampel.util.collections import try_reduce
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LightLogRecord import LightLogRecord
from ampel.log.AmpelLoggingError import AmpelLoggingError
from ampel.log.LogFlag import LogFlag
from ampel.log.utils import log_exception, report_exception

if TYPE_CHECKING:
	from ampel.core.AmpelDB import AmpelDB


# http://isthe.com/chongo/tech/comp/fnv/index.html#FNV-1a
def _fnv_1a_24(data, _ord=lambda x: x):
	"""FNV-1a 24 bit hash"""
	# http://www.isthe.com/chongo/tech/comp/fnv/index.html#xor-fold
	# Start with FNV-1a 32 bit.
	hash_size = 2 ** 32
	fnv_32_prime = 16777619
	fnv_1a_hash = 2166136261  # 32-bit FNV-1 offset basis
	for elt in data:
		fnv_1a_hash = fnv_1a_hash ^ _ord(elt)
		fnv_1a_hash = (fnv_1a_hash * fnv_32_prime) % hash_size

	# xor-fold the result to 24 bit.
	return (fnv_1a_hash >> 24) ^ (fnv_1a_hash & 0xffffff)


def _machine_bytes():
	"""Get the machine portion of an ObjectId.
	"""
	# gethostname() returns a unicode string in python 3.x
	# We only need 3 bytes, and _fnv_1a_24 returns a 24 bit integer.
	# Remove the padding byte.
	return struct.pack("<I", _fnv_1a_24(socket.gethostname().encode()))[:3]


class DBLoggingHandler(AmpelBaseModel):
	""" Saves log events into mongo database """

	__slots__ = "prev_record", "fields_check", "log_dicts", "oid_middle", "warn_lvl"

	level: int
	col_name: str = "logs"
	aggregate_interval: float = 1
	expand_extra: bool = True
	flush_len: int = 1000
	auto_flush: bool = False


	def __init__(self, ampel_db: 'AmpelDB', run_id: int, **kwargs) -> None:
		"""
		:param col_name: name of db collection to use (default: 'logs' in database Ampel_var)
		:param aggregate_interval: logs with similar attributes (log level, possibly tranId & channels) \
		are aggregated in one document instead of being split into several documents (spares some index RAM). \
		*aggregate_interval* is the max interval of time in seconds during which log aggregation takes place. \
		Beyond this value, a new log document is created no matter what. This parameter thus impacts logging time granularity.
		:param flush_len: How many log documents should be kept in memory before attempting a database bulk_write operation.
		"""

		AmpelBaseModel.__init__(self, **kwargs)
		self._ampel_db = ampel_db
		self.run_id = run_id

		self.log_dicts: list[dict] = []
		self.prev_record: None | LightLogRecord | LogRecord = None
		self.fields_check = ['extra', 'stock', 'channel']
		self.warn_lvl = LogFlag.WARNING

		# Get reference to pymongo collection
		self.col = ampel_db.get_collection(self.col_name)

		# ObjectID middle: 3 bytes machine + 2 bytes encoding the last 4 digits of run_id (unique)
		# NB: pid is not always unique if running in a jail or container
		self.oid_middle = _machine_bytes() + int(str(self.run_id)[-4:]).to_bytes(2, 'big')


	def handle(self, record: LightLogRecord | LogRecord) -> None:
		""" :raises AmpelLoggingError: on error """

		rd = record.__dict__
		prev_record = self.prev_record

		try:

			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				prev_record and
				(record.name is None or record.name == prev_record.name) and
				record.levelno <= prev_record.levelno and
				record.created - prev_record.created < self.aggregate_interval and
				compare_dict_values(rd, prev_record.__dict__, self.fields_check)
			):

				prev_dict = self.log_dicts[-1]

				if 'm' not in prev_dict:
					prev_dict['m'] = "None log entry with identical fields repeated twice"
					return

				if isinstance(prev_dict['m'], list):
					prev_dict['m'].append(record.msg)
				else:
					prev_dict['m'] = [prev_dict['m'], record.msg]

			else:

				if self.auto_flush and len(self.log_dicts) > self.flush_len:
					self.flush()

				# Treat SHOUT msg as INFO msg (and try again to concatenate)
				if record.levelno == LogFlag.SHOUT: # type: ignore
					if isinstance(record, LightLogRecord):
						new_rec = LightLogRecord(name=0, msg=None, levelno=0)
					else:
						new_rec = LogRecord(name=None, pathname=None, level=None, # type: ignore
							lineno=None, exc_info=None, msg=None, args=None) # type: ignore
					for k, v in record.__dict__.items():
						new_rec.__dict__[k] = v
					new_rec.levelno = LogFlag.INFO
					self.handle(new_rec)
					return

				# Generate object id with log record.created as current time
				with ObjectId._inc_lock:
					oid = struct.pack(">i", int(record.created)) + \
						self.oid_middle + struct.pack(">i", ObjectId._inc)[1:4]
					ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF # limit result to 32bits

				if 'extra' in rd:
					if self.expand_extra:
						for k in ('_id', 'r', 'f'):
							if k in rd['extra']:
								del rd['extra'][k]
						ldict = {
							'_id': ObjectId(oid=oid),
							'r': self.run_id,
							'f': record.levelno,
							**rd['extra']
						}
					else:
						ldict = {
							'_id': ObjectId(oid=oid),
							'r': self.run_id,
							'f': record.levelno,
							'x': rd['extra']
						}
				else:
					ldict = {
						'_id': ObjectId(oid=oid),
						'r': self.run_id,
						'f': record.levelno
					}

				if 'stock' in rd:
					ldict['s'] = rd['stock']

				if 'channel' in rd:
					ldict['c'] = try_reduce(rd['channel'])

				if record.levelno > self.warn_lvl:
					ldict['file'] = record.filename
					ldict['line'] = record.lineno

				if record.msg:
					ldict['m'] = record.msg

				self.log_dicts.append(ldict)
				self.prev_record = record

		except Exception as e:

			logger = AmpelLogger.get_logger()

			# Print log stack using std logging
			log_exception(logger, e, msg="Exception:")

			try:
				# This will fail as well if we have DB connectivity issues
				report_exception(self._ampel_db, logger, e)
			except Exception as ee:
				log_exception(
					logger, ee, last=True,
					msg="Could not update troubles collection as well (DB offline?)"
				)

			logger.error("DB log flushing error, un-flushed (json) logs below.")
			logger.error("*" * 52)

			from ampel.util.pretty import prettyjson
			for d in self.log_dicts:
				logger.error(prettyjson(d))
			logger.error("#" * 52)

			raise AmpelLoggingError from None


	def get_run_id(self) -> int:
		return self.run_id


	def check_flush(self) -> None:
		if len(self.log_dicts) > self.flush_len:
			self.flush()


	def break_aggregation(self) -> None:
		self.prev_record = None


	def flush(self) -> None:
		""" :raises AmpelLoggingError: on error """

		# No log entries
		if not self.log_dicts:
			return

		# save referenced log entries locally
		dicts = self.log_dicts

		# clear referenced log entries
		self.log_dicts = []
		self.prev_record = None

		try:
			# pymongo drops the GIL while sending and receiving data over the network
			self.col.insert_many(dicts, ordered=False)

		except BulkWriteError as bwe:
			if self.handle_bulk_write_error(bwe):
				raise AmpelLoggingError from None

		except Exception as e:
			from ampel.log.LoggingErrorReporter import LoggingErrorReporter
			LoggingErrorReporter.report(self, e)
			# If we can no longer keep track of what Ampel is doing,
			# better raise Exception to stop processing
			raise AmpelLoggingError from None


	def handle_bulk_write_error(self, bwe: BulkWriteError) -> bool:
		"""
		:returns: true if error could not be handled properly
		"""
		# We try here to handle duplicate key errors which can occur in rare scenarios.
		# We loop through each element in the returned writeErrors and
		# see for the E11000 cases if - in the DB - the runId of the log record
		# with conflicting ObjectId is the same as the one from the current instance.
		# If the runId are the same, then the conflicting log entry originates from
		# the current instance and we trust it as being the same than the one in the buffer
		# Further equality checks (msg content) could be done but we don't do them for now.

		try:

			from ampel.log.utils import get_tier_from_log_flags, convert_dollars, log_exception
			raise_exc = False

			for err_dict in bwe.details.get('writeErrors', []):

				# This log entry was already inserted into DB
				# 'code': 11000, 'errmsg': 'E11000 duplicate key error collection: ...
				if err_dict.get("code") == 11000:

					# Fetch corresponding log entry from DB
					db_rec = next(self.col.find({'_id': err_dict['op']['_id']}))

					# if runIds are equal, just print out a feedback
					if db_rec['run'] == self.run_id:
						print("Disregardable E11000: %s" % err_dict['op'])

					# Otherwise print error and create trouble doc
					else:

						raise_exc = True

						print("CRITICAL: OID collision occured between two different log entries")
						print("Current process:")
						print(err_dict['op'])
						print("In DB:")
						print(db_rec)

						self._ampel_db.get_collection('troubles').insert_one(
							{
								'tier': get_tier_from_log_flags(db_rec['f']),
								'location': 'DBLoggingHandler',
								'msg': "OID collision occured between two different log entries",
								'ownLogEntry': str(err_dict['op']),
								'existingLogEntry': str(db_rec)
							}
						)
				else:

					raise_exc = True
					print("writeError dict entry: %s" % err_dict)

					self._ampel_db.get_collection('troubles').insert_one(
						{
							'location': 'DBLoggingHandler',
							'msg': "non-E11000 writeError occured",
							'errorDict': convert_dollars(err_dict)
						}
					)

			return raise_exc

		# bad day
		except Exception:

			try:
				from traceback import format_exc
				self._ampel_db.get_collection('troubles').insert_one(
					{
						'location': 'DBLoggingHandler',
						'msg': "Exception occured in handle_bulk_write_error",
						'exception': format_exc().replace("\"", "'").split("\n")
					}
				)
			except Exception as another_exc:
				from ampel.log.AmpelLogger import AmpelLogger
				log_exception(
					AmpelLogger.get_logger(),
					another_exc
				)

			from ampel.log.LoggingErrorReporter import LoggingErrorReporter
			LoggingErrorReporter.report(self, bwe, bwe.details)

			return True
