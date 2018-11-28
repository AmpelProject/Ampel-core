#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingest/DBUpdatesBuffer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.10.2018
# Last Modified Date: 28.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
import threading
from multiprocessing.pool import ThreadPool
from pymongo.errors import BulkWriteError
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.logging.LoggingUtils import LoggingUtils


class DBUpdatesBuffer(Schedulable):
	"""
	TODO: 
	* Try to mark transient docs on error
	* Do something with self.err_ops

	Note regarding multithreading:
	PyMongo uses the standard Python socket module, 
	which does drop the GIL while sending and receiving data over the network.
	"""

	def __init__(self, run_id, logger, threads=0, push_interval=5, autopush_size=50): 
		"""
		:param int run_id:
		:param logger:
		:type logger: :py:class:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` 
		:param int push_interval: in seconds
		:param int autopush_size: 
		"""

		Schedulable.__init__(self)

		self.ops = {
			'tran': [],
			'photo': [],
			'blend': []
		}

		self.cols = {
			col_name: AmpelDB.get_collection(col_name) 
			for col_name in self.ops.keys()
		}

		self.err_ops = {k: [] for k in self.ops.keys()}
		self.run_id = run_id
		self.logger = logger
		self.autopush_size = autopush_size

		self.metrics = {
			'dbBulkTimePhoto': [],
			'dbBulkTimeBlend': [],
			'dbBulkTimeTran': [],
			'dbPerOpMeanTimePhoto': [],
			'dbPerOpMeanTimeBlend': [],
			'dbPerOpMeanTimeTran': []
		}

		self.get_scheduler().every(
			push_interval
		).seconds.do(
			self.submit_updates
		)

		if threads > 0:
			self.thread_pool = ThreadPool(threads)
			self.stop_callback = self.close_threads
		else:
			self.thread_pool = None
			self.stop_callback = self.submit_updates


	def close_threads(self):
		"""
		:returns: None
		"""
		self.submit_updates()
		self.thread_pool.close()
		self.thread_pool.join()


	def add_updates(self, updates):
		"""
		:param updates:
		:type updates: Dict[str, List[pymongo.operations]]
		:returns: None
		:raises: KeyError if dict key is unknown (known keys: photo, tran, blend)
		"""

		for k, v in updates.items():
			self.ops[k] += v
			if len(self.ops[k]) > self.autopush_size:
				if self.thread_pool:
					self.thread_pool.map(self.col_bulk_write, [k])
				else:
					self.col_bulk_write(k)


	def submit_updates(self):
		"""
		:returns: None
		"""
		for col_name in self.ops.keys():
			if self.ops[col_name]:
				if self.thread_pool:
					self.thread_pool.map(self.col_bulk_write, [col_name])
				else:
					self.col_bulk_write(col_name)


	def col_bulk_write(self, col_name, extra=None):
		"""
		:param str col_name: Ampel DB collection name (ex: photo, tran, blend)
		:returns: None

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

		There are *many* tickets opened on the mongoDB bug tracker regarding this issue.
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

		if not self.ops[col_name]:
			return

		ops = self.ops[col_name]
		self.ops[col_name] = []

		try: 

			# Update DB
			start = time()
			db_res = self.cols[col_name].bulk_write(ops, ordered=False)
			time_delta = time() - start

			# Save metrics
			self.metrics['dbBulkTime' + col_name.title()].append(time_delta)
			self.metrics['dbPerOpMeanTime' + col_name.title()].append(
				time_delta / len(ops)
			)

			self.logger.info(
				"%s: inserted: %i, upserted: %i, modified: %i" % (
					col_name,
					db_res.bulk_api_result['nInserted'],
					db_res.bulk_api_result['nUpserted'],
					db_res.bulk_api_result['nModified']
				), extra=extra
			)

		# Catch BulkWriteError only, other exceptions are caught in AlertProcessor
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
						self.cols[col_name].update_one(
							err_dict['op']['q'], 
							err_dict['op']['u'], 
							upsert=err_dict['op']['upsert']
						)
						time_delta = time() - start
	
						self.logger.info("Error recovered")
						self.metrics['dbBulkTime' + col_name.title()].append(time_delta)
						self.metrics['dbPerOpMeanTime' + col_name.title()].append(
							time_delta / len(ops)
						)
	
					else:
	
						dup_key_only = False
						self.err_ops[col_name].append(err_dict)

						LoggingUtils.report_error(
							tier=0, msg="BulkWriteError entry details", 
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
					# Do something ?
					pass
	
				self.logger.info(
					"%s: inserted: %i, upserted: %i, modified: %i, race condition(s) recovered: %i" % (
						col_name,
						bwe.details['nInserted'],
						bwe.details['nUpserted'],
						bwe.details['nModified'],
						len(bwe.details.get('writeErrors'))
					), extra=extra
				)

			except Exception as ee: 

				self.err_ops[col_name] += ops
				LoggingUtils.report_exception(
					self.logger, ee, tier=0, run_id=self.run_id
				)

		except Exception as e: 

			self.err_ops[col_name] += ops
			LoggingUtils.report_exception(
				self.logger, e, tier=0, run_id=self.run_id
			)
