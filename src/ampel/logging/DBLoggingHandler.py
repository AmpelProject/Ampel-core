#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/src/ampel/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 16.08.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, struct, os, socket
from bson import ObjectId
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateOne
from ampel.db.AmpelDB import AmpelDB
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.logging.AmpelLoggingError import AmpelLoggingError
from ampel.core.flags.LogRecordFlag import LogRecordFlag


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


class DBLoggingHandler(logging.Handler):
	"""
	Custom subclass of logging.Handler responsible for 
	saving log events into the NoSQL database.
	"""

	def __init__(
		self, flags, col_name="logs", level=logging.DEBUG, aggregate_interval=1, flush_len=1000
	):
		""" 
		:param int flags: instance of :py:class:`LogRecordFlag <ampel.core.flags.LogRecordFlag>`
		:param str col: name of db collection to use (default: 'logs' in database Ampel_var)
		:param int aggregate_interval: logs with similar attributes (log level, 
		possibly tranId & channels) are aggregated in one document instead of being split
		into several documents (spares some index RAM). *aggregate_interval* is the max interval 
		of time in seconds during which log aggregation takes place. Beyond this value, 
		a new log document is created no matter what. It thus impacts the logging time granularity.
		:param int flush_len: How many log documents should be kept in memory before
		attempting a database bulk_write operation.
		"""

		# required when not using super().__init__
		self.filters = []
		self.lock = None
		self._name = None

		# Own stuff
		self.flush_len = flush_len
		self.aggregate_interval = aggregate_interval
		self.log_dicts = []
		self.headers = []
		self.prev_records = None
		self.run_id = self.new_run_id()
		self.flags = {
			logging.DEBUG: LogRecordFlag.DEBUG | flags,
			logging.VERBOSE: LogRecordFlag.VERBOSE | flags,
			logging.INFO: LogRecordFlag.INFO | flags,
			logging.WARNING: LogRecordFlag.WARNING | flags,
			logging.ERROR: LogRecordFlag.ERROR | flags,
			# Treat critical as error
			logging.CRITICAL: LogRecordFlag.ERROR | flags
		}

		# Get reference to pymongo collection
		self.col = AmpelDB.get_collection(col_name)
			
		# Set loggingHandler properties
		self.setLevel(level)

		# ObjectID middle: 3 bytes machine + 2 bytes encoding the last 4 digits of run_id (unique)
		# NB: pid is not always unique if running in a jail or container
		self.oid_middle = _machine_bytes() + int(str(self.run_id)[-4:]).to_bytes(2, 'big')


	def new_run_id(self):
		""" 
		runId is a global (ever increasing) counter stored in the DB
		used to tie log entries from the same process with each other
		"""
		return AmpelDB.get_collection('counter').find_one_and_update(
			{'_id': 'current_run_id'},
			{'$inc': {'value': 1}},
			new=True, upsert=True
		).get('value')


	def add_headers(self, dicts):
		"""
		:param list dicts: list of dict instances
		"""
		if len(dicts) > self.flush_len:
			# We could do something about that.. later if need be
			raise ValueError("Too many logrecord headers")

		for el in dicts:
			d = el.copy()
			d['flag'] = self.flags[d.pop("lvl")]
			self.headers.append(d)


	def emit(self, record):
		""" 
		:raises AmpelLoggingError: on error
		"""

		extra = getattr(record, 'extra', None)

		try: 

			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				AmpelLogger.aggregation_ok and 
				self.prev_records and 
				record.levelno == self.prev_records.levelno and 
				record.created - self.prev_records.created < self.aggregate_interval and 
				extra == getattr(self.prev_records, 'extra', None)
			):
	
				prev_dict = self.log_dicts[-1]

				if 'msg' not in prev_dict:
					prev_dict['msg'] = "None log entry with identical 'extra' repeated twice"
					return

				if type(prev_dict['msg']) is not list:
					prev_dict['msg'] = [prev_dict['msg'], record.msg]
				else:
					prev_dict['msg'].append(record.msg)
	
			else:
	
				if len(self.log_dicts) > self.flush_len:
					self.flush()

				# Treat SHOUT msg as INFO msg (and try again to concatenate)
				if record.levelno == logging.SHOUT:
					record.levelno = logging.INFO
					self.emit(record)
					return
	
				# Generate object id with log record.created as current time
				with ObjectId._inc_lock:
					oid = struct.pack(">i", int(record.created)) + \
						  self.oid_middle + \
						  struct.pack(">i", ObjectId._inc)[1:4]
					ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF
	
				if extra:
					# If duplication exists between keys in extra and in standard rec,
					# the corresponding extra items will be overwritten (and thus ignored)
					ldict = {
						**extra, # position matters, should be first
						'_id': ObjectId(oid=oid),
						'flag': self.flags[record.levelno],
						'runId': self.run_id,
						'msg': record.msg
					}
				else:
					ldict = {
						'_id': ObjectId(oid=oid),
						'flag': self.flags[record.levelno],
						'runId': self.run_id,
						'msg': record.msg
					}

				if not record.msg:
					del ldict['msg']
	
				if record.levelno > logging.WARNING:
					ldict['filename'] = record.filename,
					ldict['lineno'] = record.lineno,
					ldict['funcName'] = record.funcName,
			
				self.log_dicts.append(ldict)
				self.prev_records = record

		except Exception as e:
			from ampel.logging.LoggingErrorReporter import LoggingErrorReporter
			LoggingErrorReporter.report(self, e)
			raise AmpelLoggingError from None


	def get_run_id(self):
		""" """
		return self.run_id


	def add_run_id(self, arg):
		""" 
		:param int arg: run ID
		"""
		if type(self.run_id) is int:
			if type(arg) is int:
				self.run_id = [self.run_id, arg]
			elif type(arg) is list:
				self.run_id = [self.run_id] + arg
			else:
				raise ValueError("Unsupported type %s" % type(arg))
		elif type(self.run_id) is list:
			if type(arg) is int:
				self.run_id.append(arg)
			elif type(arg) is list:
				self.run_id.extend(arg)
			else:
				raise ValueError("Unsupported type %s" % type(arg))
		else:
			raise ValueError("Invalid self.run_id %s" % type(self.run_id))

		# Make sure there is no duplicate
		self.run_id = list(set(self.run_id))


	def fork(self, flags):
		""" 
		:returns: new instance of DBLoggingHandler
		"""
		# New instance of DBLoggingHandler with same parameters
		dblh = DBLoggingHandler(
			flags, self.col.name, self.level, 
			self.aggregate_interval, self.flush_len
		)

		# Add runId from new instance to current instance
		self.add_run_id(
			dblh.get_run_id()
		)

		# Vice-versa
		dblh.add_run_id(
			self.get_run_id()
		)

		if not hasattr(self, "child_handlers"):
			self.child_handlers = []

		self.child_handlers.append(dblh)

		return dblh


	def flush_all(self):
		"""
		Flushes also child handlers if any
		"""
		if hasattr(self, "child_handlers"):
			for handler in self.child_handlers:
				handler.flush()
		self.flush()


	def purge(self):
		""" 
		"""
		self.log_dicts=[]
		self.prev_records = None


	def flush(self):
		""" 
		:raises AmpelLoggingError: on error
		"""

		# No log entries
		if not self.log_dicts:
			return

		# save referenced log entries locally
		dicts = self.log_dicts 

		# clear referenced log entries
		self.log_dicts = []
		self.prev_records = None

		try:

			if self.headers:
			
				# following command drops the GIL
				self.col.bulk_write(
					[
						UpdateOne(
							{'_id': rec['_id']},
							{
								'$setOnInsert': rec,
								'$addToSet': {
									'runId': {'$each': self.run_id}
								} if type(self.run_id) is list
								else {
									'runId': self.run_id
								}
							},
							upsert=True
						)
						for rec in self.headers
					],
					ordered=False
				)
				self.headers = None

			# pymongo drops the GIL while sending and receiving data over the network
			self.col.insert_many(dicts)

		except BulkWriteError as bwe:
			if self.handle_bulk_write_error(bwe):
				raise AmpelLoggingError from None

		except Exception as e:
			from ampel.logging.LoggingErrorReporter import LoggingErrorReporter
			LoggingErrorReporter.report(self, e)
			# If we can no longer keep track of what Ampel is doing, 
			# better raise Exception to stop processing
			raise AmpelLoggingError from None


	def handle_bulk_write_error(self, bwe):
		"""
		:returns: true if error could not be handled properly
		:param BulkWriteError bwe:
		"""
		# We try here to handle duplicate key errors as they could occur 
		# in weird scenarios (quick connection issues for example). 
		# we loop through each element in the returned writeErrors and 
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
					if db_rec['runId'] == self.run_id:
						print("Disregardable E11000: "+str(err_dict['op']))

					# Otherwise print error and create trouble doc
					else:

						raise_exc = True

						print("CRITICAL: OID collision occured between two different log entries")
						print("Current process:")
						print(err_dict['op'])
						print("In DB:")
						print(db_rec)

						AmpelDB.get_collection('troubles').insert_one(
							{
								'tier': LoggingUtils.get_tier_from_log_flags(self.flags),	
								'location': 'DBLoggingHandler',
								'msg': "OID collision occured between two different log entries",
								'ownLogEntry': str(err_dict['op']),
								'existingLogEntry': str(db_rec)
							}
						)
				else:

					raise_exc = True
					print("writeError dict entry: " + str(err_dict))

					AmpelDB.get_collection('troubles').insert_one(
						{
							'tier': LoggingUtils.get_tier_from_log_flags(self.flags),	
							'location': 'DBLoggingHandler',
							'msg': "non-E11000 writeError occured",
							'errorDict': LoggingUtils.convert_dollars(err_dict)
						}
					)

			return raise_exc

		# bad day
		except Exception as ee:

			try: 
				from traceback import format_exc
				AmpelDB.get_collection('troubles').insert_one(
					{
						'tier': LoggingUtils.get_tier_from_log_flags(self.flags),	
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
