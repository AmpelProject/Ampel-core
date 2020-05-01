#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 01.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, struct, socket
from bson import ObjectId
from typing import List, Dict, Optional, Union
from logging import LogRecord
from pymongo.errors import BulkWriteError
from ampel.db.AmpelDB import AmpelDB
from ampel.utils.mappings import compare_dict_values
from ampel.logging.LighterLogRecord import LighterLogRecord
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.logging.AmpelLoggingError import AmpelLoggingError
from ampel.logging.LogRecordFlag import LogRecordFlag


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


class DBLoggingHandler:
	"""
	Class capable of saving log events into the NoSQL database.
	"""

	def __init__(self,
		ampel_db: AmpelDB,
		flags: LogRecordFlag,
		col_name: str = "logs",
		level: int = logging.DEBUG,
		aggregate_interval: float = 1,
		expand_extra: bool = True,
		flush_len: int = 1000
	):
		"""
		:param col_name: name of db collection to use (default: 'logs' in database Ampel_var)
		:param aggregate_interval: logs with similar attributes (log level, possibly tranId & channels) \
		are aggregated in one document instead of being split into several documents (spares some index RAM). \
		*aggregate_interval* is the max interval of time in seconds during which log aggregation takes place. \
		Beyond this value, a new log document is created no matter what. This parameter thus impacts logging time granularity.
		:param flush_len: How many log documents should be kept in memory before attempting a database bulk_write operation.
		"""

		# required when not using super().__init__
		#self.filters = []
		#self.lock = None
		#self._name = None

		self._ampel_db = ampel_db
		self.flush_len = flush_len
		self.aggregate_interval = aggregate_interval
		self.log_dicts: List[Dict] = []
		self.prev_record: Optional[Union[LighterLogRecord, LogRecord]] = None
		self.run_id: int = self.new_run_id()
		self.fields_check = ['extra', 'stock', 'channel']
		self.expand_extra = expand_extra
		self.warn_lvl = logging.WARNING
		self.flags = {
			logging.DEBUG: LogRecordFlag.DEBUG | flags,
			# we use/set our own logging level (we add VERBOSE as well)
			logging.VERBOSE: LogRecordFlag.VERBOSE | flags, # type: ignore
			logging.SHOUT: LogRecordFlag.INFO | flags, # type: ignore
			logging.INFO: LogRecordFlag.INFO | flags,
			logging.WARNING: LogRecordFlag.WARNING | flags,
			logging.ERROR: LogRecordFlag.ERROR | flags,
			logging.CRITICAL: LogRecordFlag.ERROR | flags # Treat critical as error
		}

		# Get reference to pymongo collection
		self.col = ampel_db.get_collection(col_name)

		# Set loggingHandler properties
		#self.setLevel(level)
		self.level = level

		# ObjectID middle: 3 bytes machine + 2 bytes encoding the last 4 digits of run_id (unique)
		# NB: pid is not always unique if running in a jail or container
		self.oid_middle = _machine_bytes() + int(str(self.run_id)[-4:]).to_bytes(2, 'big')


	def new_run_id(self) -> int:
		"""
		run_id is a global (ever increasing) counter stored in the DB \
		used to tie log entries from the same process with each other
		"""
		return self._ampel_db \
			.get_collection('counter') \
			.find_one_and_update(
				{'_id': 'current_run_id'},
				{'$inc': {'value': 1}},
				new=True, upsert=True
			) \
			.get('value')


	def handle(self, record: Union[LighterLogRecord, LogRecord]) -> None:
		""" :raises AmpelLoggingError: on error """

		rd = record.__dict__

		try:

			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				self.prev_record and
				(record.name is None or record.name == self.prev_record.name) and
				record.levelno <= self.prev_record.levelno and
				record.created - self.prev_record.created < self.aggregate_interval and
				compare_dict_values(rd, self.prev_record.__dict__, self.fields_check)
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

				if len(self.log_dicts) > self.flush_len:
					self.flush()

				# Treat SHOUT msg as INFO msg (and try again to concatenate)
				if record.levelno == logging.SHOUT: # type: ignore
					if isinstance(record, LighterLogRecord):
						new_rec = LighterLogRecord(name=0, msg=None, levelno=0)
					else:
						new_rec = LogRecord(name=None, pathname=None, level=None, # type: ignore
							lineno=None, exc_info=None, msg=None, args=None) # type: ignore
					for k, v in record.__dict__.items():
						new_rec.__dict__[k] = v
					new_rec.levelno = logging.INFO
					self.handle(new_rec)
					return

				# Generate object id with log record.created as current time
				with ObjectId._inc_lock:
					oid = struct.pack(">i", int(record.created)) + \
						self.oid_middle + struct.pack(">i", ObjectId._inc)[1:4]
					ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF # limit result to 32bits

				if 'extra' in rd:
					if self.expand_extra:
						ldict = {
							**rd['extra'],
							'_id': ObjectId(oid=oid),
							'f': self.flags[record.levelno],
							'r': self.run_id,
						}
					else:
						ldict = {
							'_id': ObjectId(oid=oid),
							'f': self.flags[record.levelno],
							'r': self.run_id,
							'x': rd['extra']
						}
				else:
					ldict = {
						'_id': ObjectId(oid=oid),
						'f': self.flags[record.levelno],
						'r': self.run_id,
					}

				if record.msg:
					ldict['m'] = record.msg

				if 'channel' in rd:
					ldict['c'] = rd['channel']

				if 'stock' in rd:
					ldict['s'] = rd['stock']

				if record.levelno > self.warn_lvl:
					ldict['file'] = record.filename
					ldict['line'] = record.lineno

				self.log_dicts.append(ldict)
				self.prev_record = record

		except Exception as e:
			from ampel.logging.LoggingErrorReporter import LoggingErrorReporter
			LoggingErrorReporter.report(self, e)
			raise AmpelLoggingError from None


	def get_run_id(self) -> int:
		return self.run_id


	def purge(self) -> None:
		self.log_dicts = []
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
			from ampel.logging.LoggingErrorReporter import LoggingErrorReporter
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
								'tier': LoggingUtils.get_tier_from_log_flags(db_rec['f']),
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
							'tier': LoggingUtils.get_tier_from_log_flags(self.flags[logging.DEBUG]),
							'location': 'DBLoggingHandler',
							'msg': "non-E11000 writeError occured",
							'errorDict': LoggingUtils.convert_dollars(err_dict)
						}
					)

			return raise_exc

		# bad day
		except Exception:

			try:
				from traceback import format_exc
				self._ampel_db.get_collection('troubles').insert_one(
					{
						'tier': LoggingUtils.get_tier_from_log_flags(self.flags[logging.DEBUG]),
						'location': 'DBLoggingHandler',
						'msg': "Exception occured in handle_bulk_write_error",
						'exception': format_exc().replace("\"", "'").split("\n")
					}
				)
			except Exception as another_exc:
				LoggingUtils.log_exception(
					AmpelLogger.get_unique_logger(),
					another_exc
				)

			from ampel.logging.LoggingErrorReporter import LoggingErrorReporter
			LoggingErrorReporter.report(self, bwe, bwe.details)

			return True
