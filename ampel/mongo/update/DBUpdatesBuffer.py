#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/DBUpdatesBuffer.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                31.10.2018
# Last Modified Date:  22.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, Lock
from typing import Any, Literal

from pymongo import InsertOne, UpdateMany, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from ampel.core.AmpelDB import AmpelDB, intcol
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.utils import convert_dollars, report_error, report_exception
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry

DBOp = UpdateOne | UpdateMany | InsertOne
AmpelMainCol = Literal['stock', 't0', 't1', 't2', 't3']

# Monitoring counters
stat_db_ops = AmpelMetricsRegistry.counter(
	"ops",
	"Number of bulk operations",
	subsystem="db",
	labelnames=("col",)
)
stat_db_errors = AmpelMetricsRegistry.counter(
	"errors",
	"Number of bulk op errors",
	subsystem="db",
	labelnames=("col",)
)
stat_db_time = AmpelMetricsRegistry.histogram(
	"time",
	"Latency of bulk operations",
	unit="seconds",
	subsystem="db",
	labelnames=("col",)
)

class DBUpdatesBuffer:
	"""
	TODO:
	* Try to mark transient docs on error
	* Do something with self._err_db_ops

	Note1:
	Regarding multithreading: pymongo uses the standard Python socket module,
	which can drop the GIL while sending and receiving data over the network.

	Note2:
	On the advantages of buffering updates and submitting them in bulk

	-> Without buffering:
	In []:
		start=time()
		for i in range(200000):
			col.insert_one({'_id':i})
		print(f"{round(time()-start, 1)}s")
	Out []: 60.4s


	-> With buffering:
	In []:
		ops=[]
		start=time()
		for i in range(200000):
			ops.append(InsertOne({'_id': i}))
			if i % 2000 == 0:
				col.bulk_write(list(ops), ordered=False)
				ops=[]
		col.bulk_write(ops, ordered=False)
		print(f"{round(time()-start, 1)}s")
	Out []: 1.7s
	"""

	def __init__(self,
		ampel_db: AmpelDB,
		run_id: int | list[int],
		logger: AmpelLogger,
		error_callback: None | Callable[[], None] = None,
		acknowledge_callback: None | Callable[[Iterator[Any]], None] = None,
		log_doc_ids: None | Iterable[int] = None,
		push_interval: None | float = 3,
		max_size: None | int = None,
		raise_exc: bool = False
	):
		"""
		:param error_callback: callback method to be called on errors
		:param acknowledge_callback: method to call to acknowledge processing of a message bunch
		:param catch_signals: see Schedulable docstring
		:param log_doc_ids: logs inserted/updated document IDs for the given collections.
		Collection integer identifiers are: 't0' -> 0, 't1' -> 1, 't2' -> 2, 'stock' -> 3.
		Thus, if you want to activate this behavior for all collections, use log_doc_ids=[0, 1, 2, 3].
		Notes: 1) it this will significantly increase the log size/amount.
		2) the "_id" of T2 documents is generated server-side and bulk_write does not return it,
		so it is not avail 'as is'. We instead log the natching filter (t2id, config, link)
		which is guaranteed to be unique, so it's an equivalent.
		3) Huge values of push_interval can yield a document size overflow (16MB limit per doc)
		:param push_interval: If provided, updates will be flushed every x seconds.
		:param max_size: If provided, updates will be flushed as soon as possible after the
		number of updated in any collection (stock, t0, t1, t2) exceeds the provided limit.
		:param threads: perform the various collection bound bulk_write() operations in dedicated threads.
		The provided integer number defines the size of the thread pool. Note that no real performance gain
		was yet noticed using a meaningful value such as 4 (OSX, python 3.8.1). Since bulk_write drops the GIL,
		a multithreading-like effect already occurs without the specific use a threads.
		"""

		self._new_buffer()
		self.error_callback = error_callback
		self.acknowledge_callback = acknowledge_callback

		self._cols: dict[AmpelMainCol, Collection] = {
			col_name: ampel_db.get_collection(col_name)
			for col_name in self.db_ops
		}

		self.stats: dict[AmpelMainCol, int] = {
			'stock': 0, 't0': 0, 't1': 0, 't2': 0,
		}

		self._err_db_ops: dict[str, Any] = {k: [] for k in self.db_ops}
		self._ampel_db = ampel_db
		self.run_id = run_id
		self.logger = logger
		self.max_size = max_size
		self.log_doc_ids = set(log_doc_ids) if log_doc_ids else None

		# wake up update pusher thread
		self._push = Event()
		# prevent autopush while we are in the group_updates context
		self._block_autopush = Lock()
		self._pushing = Lock()
		# signal update pusher thread to stop
		self._stop = Event()
		self._exec = ThreadPoolExecutor()

		self.push_interval = push_interval
		self.raise_exc = raise_exc


	def _new_buffer(self) -> None:
		""" Creates new buffer for pymongo operations """

		# total number of updates (of all kinds/collections)
		self.db_ops: dict[AmpelMainCol, list[DBOp]] = {
			't2': [], 't0': [], 'stock': [], 't1': []
		}
		self._messages_to_ack: list[Any] = []

	def __enter__(self) -> None:
		"""
		:raises: RuntimeError if context manager is already running
		"""
		self._stop.clear()
		def task():
			while not self._stop.is_set():
				self._push.wait(self.push_interval)
				self._push.clear()
				self.push_updates(force=True)
		self._task = self._exec.submit(task)
	
	def __exit__(self, exc_type: type[BaseException], exc_value: BaseException, traceback: Any) -> None:
		"""
		:raises: RuntimeError if context manager is already running
		"""
		# stop updater thread
		self._stop.set()
		self._push.set()
		self._task.result()
		# push any remaining updates
		self.push_updates()


	def acknowledge_on_push(self, message: Any) -> None:
		"""
		Add a message to be passed to acknowledge_callback when the current
		buffer has been pushed
		"""
		if self.acknowledge_callback:
			self._messages_to_ack.append(message)


	def add_updates(self, updates: dict[AmpelMainCol, list[DBOp]]) -> None:
		"""
		:raises: KeyError if dict key is unknown (known keys: stock, t0, t1, t2)
		"""
		for k, v in updates.items():
			self.db_ops[k] += v


	def add_col_updates(self, col: AmpelMainCol, updates: list[DBOp]) -> None:
		""" :raises: KeyError if col is unknown (known cols: stock, t0, t1, t2) """
		self.db_ops[col] += updates


	def add_col_update(self, col: AmpelMainCol, update: DBOp) -> None:
		""" :raises: KeyError if col is unknown (known cols: stock, t0, t1, t2) """
		self.db_ops[col].append(update)


	def add_t0_update(self, update: DBOp) -> None:
		self.db_ops['t0'].append(update)


	def add_t1_update(self, update: DBOp) -> None:
		self.db_ops['t1'].append(update)


	def add_t2_update(self, update: DBOp) -> None:
		self.db_ops['t2'].append(update)


	def add_stock_update(self, update: DBOp) -> None:
		self.db_ops['stock'].append(update)


	@contextmanager
	def group_updates(self) -> Generator:
		"""
		Ensure that updates issued in this context are grouped together
		"""
		with self._block_autopush:
			yield
			self.check_push()
		# potentially raise exceptions to main thread
		if self._task.done():
			self._task.result()


	def check_push(self) -> None:
		if self.max_size is not None and any(
			len(v) > self.max_size
			for v in self.db_ops.values()
		):
			# block until the previous buffer is pushed, then signal the thread
			# to swap and push the new one
			with self._pushing:
				self._push.set()


	def push_updates(self, force: bool = False) -> None:

		# swap buffers
		with self._block_autopush:
			db_ops, messages = self.db_ops, self._messages_to_ack
			self._new_buffer()

		# prevent the new buffer from overfilling before bulk writes complete
		with self._pushing:
			for _ in self._exec.map(
				self.call_bulk_write,
				*zip(*((col_name, ops) for col_name, ops in db_ops.items() if ops), strict=True)
			):
				pass

			if self.acknowledge_callback and messages:
				try:
					self.acknowledge_callback(iter(messages))
				except Exception as exc:
					if self.raise_exc:
						raise
					report_exception(self._ampel_db, self.logger, exc=exc)


	def call_bulk_write(self, col_name: AmpelMainCol, db_ops: list, *, extra: None | dict = None) -> None:
		"""
		:param col_name: Ampel DB collection name (ex: stock, t0, t1, t2)
		:param db_ops: list of pymongo operations
		:raises: None, but stops the AlertConsumer processing by using the method
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
		
		with stat_db_time.labels(col_name).time():
			try:

				# Update DB
				db_res = self._cols[col_name].bulk_write(db_ops, ordered=False)
				stat_db_ops.labels(col_name).inc(len(db_ops))

				self.logger.debug(
					None, extra=self._build_log_extra(
						col_name, db_ops, db_res.bulk_api_result, extra
					)
				)

				return

			except BulkWriteError as bwe:

				try:

					dup_key_only = True

					for err_dict in bwe.details.get('writeErrors', []):

						stat_db_errors.labels(col_name).inc()
						# 'code': 11000, 'errmsg': 'E11000 duplicate key error collection: ...
						if err_dict.get("code") == 11000:

							self.logger.info(
								f"Race condition during ingestion in '{col_name}': {err_dict}"
							)
							self.logger.flush()

							# Should no longer raise pymongo.errors.DuplicateKeyError
							self._cols[col_name].update_one(
								err_dict['op']['q'],
								err_dict['op']['u'],
								upsert=err_dict['op']['upsert']
							)
							stat_db_ops.labels(col_name).inc()

						else:

							dup_key_only = False
							self._err_db_ops[col_name].append(err_dict)

							# Try to insert doc into trouble collection (raises no exception)
							# Possible exception will be logged out to console in any case
							report_error(
								self._ampel_db, msg="BulkWriteError entry details",
								logger=self.logger, info={
									'run': self.run_id,
									'err': convert_dollars(err_dict)
								}
							)

							################################
							# TODO: better than this.
							# - Mark corresponding transients with an error flag (add channel info?)
							# - Implement something for temp DB connectivity issues ?
							################################

							if self.raise_exc:
								raise

					if dup_key_only:
						self.logger.debug(
							f"Race condition(s) recovered: {len(bwe.details.get('writeErrors', []))}",
							extra=self._build_log_extra(col_name, db_ops, bwe.details, extra)
						)

						return

				except Exception as ee:
					if self.raise_exc:
						raise
					# Log exc and try to insert doc into trouble collection (raises no exception)
					report_exception(self._ampel_db, self.logger, exc=ee)

			except Exception as e:
				if self.raise_exc:
					raise
				# Log exc and try to insert doc into trouble collection (raises no exception)
				report_exception(self._ampel_db, self.logger, exc=e)

			print(f"Update of {col_name} collection has failed")  # noqa: T201
			print(db_ops)  # noqa: T201

			self._err_db_ops[col_name] += db_ops
			if self.error_callback:
				self.error_callback()


	def _build_log_extra(self,
		col_name: str,
		ops: list[DBOp],
		bulk_api_result: Mapping[str, Any],
		extra: None | dict[str, Any] = None
	) -> dict[str, Any]:

		ret = {
			'col': intcol[col_name],
			'ins': bulk_api_result['nInserted'],
			'ups': bulk_api_result['nUpserted'],
			'mod': bulk_api_result['nModified']
		}

		if self.log_doc_ids and ret['col'] in self.log_doc_ids:
			try:
				if ret['col'] != 2:
					ret['docs'] = [op._filter['_id'] for op in ops] # type: ignore[union-attr]  # noqa: SLF001
				else:
					ret['docs'] = [
						{
							'unit': op._filter['unit'], # type: ignore[union-attr]  # noqa: SLF001
							'config': op._filter['config'], # type: ignore[union-attr]  # noqa: SLF001
							'link': op._filter['link'] # type: ignore[union-attr]  # noqa: SLF001
						}
						for op in ops
					]
			except Exception:
				pass

		if extra:
			return {**extra, **ret}

		return ret
