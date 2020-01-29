#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t0/ingest/DBUpdatesBuffer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.10.2018
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo.errors import BulkWriteError
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Any, Union, Callable

from ampel.core.Schedulable import Schedulable
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.db.AmpelDB import AmpelDB


class DBUpdatesBuffer(Schedulable):
	"""
	TODO:
	* Try to mark transient docs on error
	* Do something with self._err_db_ops

	Note regarding multithreading:
	PyMongo uses the standard Python socket module,
	which does drop the GIL while sending and receiving data over the network.
	"""

	def __init__(self,
		ampel_db: AmpelDB, run_id: Union[int, List[int]],
		logger: AmpelLogger, on_error_func: Callable[[], None],
		threads: int = 8, push_interval: int = 5, autopush_size: int = 100
	):
		"""
		:param on_error_func: callback method to be called on errors
		:param push_interval: in seconds
		"""

		Schedulable.__init__(self)
		self._new_buffer()
		self.on_error_func = on_error_func

		self._cols = {
			col_name: ampel_db.get_collection(col_name)
			for col_name in self.db_ops.keys()
		}

		self._err_db_ops: Dict[str, Any] = {k: [] for k in self.db_ops.keys()}
		self._ampel_db = ampel_db
		self._last_check = time()
		self._size = 0
		self.run_id = run_id
		self.logger = logger
		self.push_interval = push_interval
		self.autopush_size = autopush_size

		self.metrics: Dict[str, List] = {
			'dbBulkTimeT0': [],
			'dbBulkTimeT1': [],
			'dbBulkTimeT2': [],
			'dbBulkTimeStock': [],
			'dbPerOpMeanTimeT0': [],
			'dbPerOpMeanTimeT1': [],
			'dbPerOpMeanTimeT2': [],
			'dbPerOpMeanTimeStock': []
		}

		if push_interval:
			self.get_scheduler() \
				.every(push_interval) \
				.seconds.do(self.auto_push_updates)

		self.thread_pool = ThreadPool(threads) if threads else None
		self.stop_callback = self.close


	def _new_buffer(self) -> None:
		""" """
		self.db_ops: Dict[str, List] = {
			'stock': [],
			't0': [],
			't1': [],
			't2': []
		}


	def close(self) -> None:
		"""
		:returns: None
		"""
		self.push_updates()

		if self.thread_pool:
			self.thread_pool.close()
			self.thread_pool.join()


	def add_updates(self, updates: Dict[str, List]) -> None:
		"""
		:param updates: Dict[str, List[<pymongo.operations>]]
		:raises: KeyError if dict key is unknown (known keys: stock, t0, t1, t2)
		"""
		for k, v in updates.items():
			self.db_ops[k] += v
			self._size += len(v)


	def auto_push_updates(self) -> None:
		"""
		:returns: None
		"""
		if time() - self._last_check < self.push_interval:
			return

		self.push_updates()


	def ap_push_updates(self) -> None:
		"""
		Called by the AlertProcessor after the processing of an alert.
		If the pool is not yet big enough (self.autopush_size)
		the self._last_check is updated.
		By doing that, the regularly scheduled auto_push_updates()
		will be delayed as long as the AP processes alerts.
		:returns: None
		"""

		if self._size < self.autopush_size:
			self._last_check = time()
			return

		self.push_updates()


	def push_updates(self) -> None:
		""" """

		self._last_check = time()

		if self._size == 0:
			return

		db_ops = self.db_ops
		self._new_buffer()
		self._size = 0

		for col_name in db_ops.keys():
			if db_ops[col_name]:
				if self.thread_pool:
					self.thread_pool.starmap(
						self.call_bulk_write,
						([col_name, db_ops[col_name]], )
					)
				else:
					self.call_bulk_write(col_name, db_ops[col_name])


	def call_bulk_write(self, col_name: str, db_ops: List, extra: Dict = None) -> None:
		"""
		:param col_name: Ampel DB collection name (ex: stock, t0, t1, t2)
		:param db_ops: list of pymongo operations
		:raises: None, but stops the AlertProcessor processing by using the method
		cancel_run() when unrecoverable exceptions occur.

		Regarding the handling of BulkWriteError:
		Concurent upserts triggers a DuplicateKeyError exception.

		https://stackoverflow.com/questions/37295648/mongoose-duplicate-key-error-with-upsert
		<quote>
			An upsert that results in a document insert is not a fully atomic operation.
			Think of the upsert as performing the following discrete steps:
    			Query for the identified document to upsert.
    			If the document exists, atomically update the existing document.
    			Else (the document doesn't exist), atomically insert a new document
				that incorporates the query fields and the update.
		</quote>

		There are many tickets opened on the mongoDB bug tracker regarding this issue.
		One of which: https://jira.mongodb.org/browse/SERVER-14322
		where is stated:
			"It is expected that the client will take appropriate action
			upon detection of such constraint violation"

		All in all: the server behaves inappropriately, the driver won't catch those
		cases for us, so we have to do the work by ourself.

		Last: the use of SON (serialized Ocument Normalisation) is deprecated according
		to the mongoDB doc. It will be removed with pymongo 4, so we should not use it anymore.
		BUT: the offending updates (UpdateOne instances) returned by the server are
		provided as SON by BulkWriteError (array 'writeErrors' contains SON objects).
		So we have no other choice than handling with them for now.
		"""

		try:

			# Update DB
			start = time()
			db_res = self._cols[col_name].bulk_write(db_ops, ordered=False)
			time_delta = time() - start

			# Save metrics
			self.metrics['dbBulkTime' + col_name.title()].append(time_delta)
			self.metrics['dbPerOpMeanTime' + col_name.title()].append(
				time_delta / len(db_ops)
			)

			self.logger.debug(
				f"{col_name}: inserted: {db_res.bulk_api_result['nInserted']}, "
				f"upserted: {db_res.bulk_api_result['nUpserted']}, "
				f"modified: {db_res.bulk_api_result['nModified']}",
				extra=extra
			)

			return

		except BulkWriteError as bwe:

			try:

				dup_key_only = True

				for err_dict in bwe.details.get('writeErrors', []):

					# 'code': 11000, 'errmsg': 'E11000 duplicate key error collection: ...
					if err_dict.get("code") == 11000:

						self.logger.info(
							"Race condition during ingestion in '%s': %s" % (
								col_name, err_dict
							)
						)

						# Should no longer raise pymongo.errors.DuplicateKeyError
						start = time()
						self._cols[col_name].update_one(
							err_dict['op']['q'],
							err_dict['op']['u'],
							upsert=err_dict['op']['upsert']
						)
						time_delta = time() - start

						self.logger.info("Error recovered")
						self.metrics['dbBulkTime' + col_name.title()].append(time_delta)
						self.metrics['dbPerOpMeanTime' + col_name.title()].append(
							time_delta / len(db_ops)
						)

					else:

						dup_key_only = False
						self._err_db_ops[col_name].append(err_dict)

						# Try to insert doc into trouble collection (raises no exception)
						# Possible exception will be logged out to console in any case
						LoggingUtils.report_error(
							self._ampel_db, tier=0, msg="BulkWriteError entry details",
							logger=self.logger, info={
								'run_id': self.run_id,
								'errDict': LoggingUtils.convert_dollars(err_dict)
							}
						)

						################################
						# TODO: do better than this.
						# - Mark corresponding transients with an error flag (add channel info?)
						# - Implement something for temp DB connectivity issues ?
						################################


				if dup_key_only:
					self.logger.debug(
						f"{col_name}: inserted: {bwe.details['nInserted']}, "
						f"upserted: {bwe.details['nUpserted']}, modified: {bwe.details['nModified']}, "
						f"race condition(s) recovered: {len(bwe.details.get('writeErrors'))}",
						extra=extra
					)

					return

			except Exception as ee:
				# Log exc and try to insert doc into trouble collection (raises no exception)
				LoggingUtils.report_exception(
					self._ampel_db, self.logger, tier=0,
					exc=ee, run_id=self.run_id
				)

		except Exception as e:
			# Log exc and try to insert doc into trouble collection (raises no exception)
			LoggingUtils.report_exception(
				self._ampel_db, self.logger, tier=0,
				exc=e, run_id=self.run_id
			)

		self._err_db_ops[col_name] += db_ops
		self.on_error_func()
