import gc
from collections.abc import Generator, Sequence
from time import time
from typing import Any, Literal, TypedDict, overload

from bson import ObjectId
from mongomock.filtering import filter_applies

from ampel.abstract.AbsIngester import AbsIngester
from ampel.abstract.AbsWorker import stat_time
from ampel.content.DataPoint import DataPoint
from ampel.content.MetaRecord import MetaRecord
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.core.EventHandler import EventHandler
from ampel.enum.DocumentCode import DocumentCode
from ampel.log import AmpelLogger, LogFlag
from ampel.model.UnitModel import UnitModel
from ampel.queue.AbsConsumer import AbsConsumer
from ampel.t2.T2Worker import T2Worker
from ampel.types import (
	DataPointId,
	OneOrMany,
	StockId,
	T2Link,
	Tag,
	UBson,
)


class QueueItem(TypedDict):
	stock: Sequence[StockDocument]
	t0: Sequence[DataPoint]
	t1: Sequence[T1Document]
	t2: Sequence[T2Document]



class T2QueueWorker(T2Worker):

	consumer: UnitModel
	ingester: UnitModel = UnitModel(unit="MongoIngester")
	
	# Must run dependent t2s, as no other worker will see them
	run_dependent_t2s: Literal[True] = True
	pre_check: Literal[False] = False

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)

		self._current_item: None | QueueItem = None
	
	def proceed(self, event_hdlr: EventHandler) -> int:
		""" :returns: number of t2 docs processed """

		event_hdlr.set_tier(self.tier)
		run_id = event_hdlr.get_run_id()

		if self.send_beacon:
			self.col_beacon.update_one(
				{'_id': self.beacon_id},
				{'$set': {'timestamp': int(time())}}
			)

		# Loop variables
		doc_counter = 0
		garbage_collect = self.garbage_collect
		doc_limit = self.doc_limit

		with (
			AmpelLogger.from_profile(
				self.context, self.log_profile, run_id,
				base_flag = getattr(LogFlag, f'T{self.tier}') | LogFlag.CORE | self.base_log_flag
			) as logger,
			self._run_until_signal(run_id, logger) as stop_token,
			self.context.loader.new(
				self.consumer,
				stop=stop_token,
				unit_type=AbsConsumer,
			) as consumer,
			self.context.loader.new_context_unit(
				self.ingester,
				context = self.context,
				run_id = run_id,
				tier = self.tier,
				process_name = self.process_name,
				error_callback = stop_token.set,
				acknowledge_callback = consumer.acknowledge,
				logger = logger,
				sub_type = AbsIngester,
			) as ingester
		):

			# Process docs until next() returns None (breaks condition below)
			while not stop_token.is_set():

				# get t1/t2 document (code is usually NEW or NEW_PRIO), excluding
				# docs with retry times in the future
				with stat_time.labels(self.tier, "consume", None).time():
					item: None | QueueItem = consumer.consume()

				# No match
				if item is None:
					if not stop_token.is_set():
						logger.log(LogFlag.SHOUT, "No more docs to process")
					break

				self._current_item = item

				input_docs = item["t2"]
				# Replace inputs with resolved version from the database.
				# Doing this here allows process_doc() to find
				# already-processed docs if it recurse into
				# load_input_docs() for tied units.
				item["t2"] = [self._sub_existing_doc(doc) for doc in item["t2"]]

				with ingester.group([item]):
					for input_doc, doc in zip(input_docs, item["t2"], strict=True):
						if doc is input_doc:
							# not found in the database; process for the first time
							with stat_time.labels(self.tier, "process_doc", doc["unit"]).time():
								self.process_doc(doc, ingester, logger)
					doc_counter += 1

					for stock in item["stock"]:
						ingester.stock.ingest(stock)
					for dp in item["t0"]:
						ingester.t0.ingest(dp)
					for t1 in item["t1"]:
						ingester.t1.ingest(t1)
					# NB: ingest the input docs as they were before
					# substitution in order to pick up any requested updates
					# to meta, channels, tags, expiry, etc.
					for t2 in input_docs:
						ingester.t2.ingest(t2)

				# Check possibly defined doc_limit
				if doc_limit and doc_counter >= doc_limit:
					break

				if garbage_collect:
					gc.collect()

		event_hdlr.add_extra(docs=doc_counter)
		return doc_counter

	def _sub_existing_doc(self, doc: T2Document) -> T2Document:
		"""replace doc with a resolved copy from the database if it exists"""
		match = {
			'code': DocumentCode.OK,
			'stock': doc['stock'],
			'unit': doc['unit'],
			'config': doc['config'],
			'link': doc['link']
		}

		if 'origin' in doc:
			match['origin'] = doc['origin']

		db_doc: None | T2Document
		if db_doc := self.col.find_one(match):
			# merge the existing doc with the new one for consistency
			db_doc["meta"] = [*db_doc["meta"], *doc.get("meta", [])]
			db_doc["body"] = [*db_doc["body"], *doc.get("body", [])]
			db_doc["tag"] = list(set(db_doc.get("tag", [])).union(doc.get("tag", [])))
			db_doc["channel"] = list(set(db_doc.get("channel", [])).union(doc.get("channel", [])))

			return db_doc
		return doc

	def update_doc(self,
		doc: T2Document,
		meta: MetaRecord,
		logger: AmpelLogger, *,
		body: UBson = None,
		tag: None | OneOrMany[Tag] = None,
		code: int = 0
	) -> None:
		# update doc in place; changes will be propagated by the ingester as a block
		doc["code"] = code
		doc["meta"] = [*doc["meta"], meta]
		if body:
			doc["body"] = [*doc["body"], body]
		if tag:
			doc["tag"] = list(
				set([tag] if isinstance(tag, Tag) else tag)
				.union(doc.get("tag", []))
			)

	def load_stock(self, stock: StockId) -> None | StockDocument:
		"""Load stock document from current message"""
		return (
			next((d for d in self._current_item["stock"] if d["stock"] == stock), None)
			if self._current_item is not None else None
		) or super().load_stock(stock)

	@overload
	def load_t0(self, stock: StockId | Sequence[StockId], t1_dps_ids: DataPointId) -> None | DataPoint: ...

	@overload
	def load_t0(self, stock: StockId | Sequence[StockId], t1_dps_ids: Sequence[DataPointId]) -> list[DataPoint]: ...

	def load_t0(self, stock: StockId | Sequence[StockId], t1_dps_ids: DataPointId | Sequence[DataPointId]) -> None | DataPoint | list[DataPoint]:
		"""Load datapoints from database"""
		if isinstance(t1_dps_ids, DataPointId):
			return (
				next((d for d in self._current_item["t0"] if d["stock"] == stock and d["id"] == t1_dps_ids), None)
				if self._current_item is not None else None
			) or super().load_t0(stock, t1_dps_ids)
		targets = set(t1_dps_ids)
		datapoints = (
			{dp["id"]: dp for dp in self._current_item["t0"] if dp["stock"] == stock and dp["id"] in targets}
			if self._current_item is not None else {}
		)
		# fall back to database if some datapoints are missing
		if missing := targets.difference(datapoints.keys()):
			return list(datapoints.values()) + super().load_t0(stock, list(missing))
		return list(datapoints.values())

	def load_t1(self, stock: StockId | Sequence[StockId], link: T2Link) -> None | T1Document:
		"""Load T1 document from database"""
		return (
			next((d for d in self._current_item["t1"] if d["stock"] == stock and d["link"] == link), None)
			if self._current_item is not None else None
		) or super().load_t1(stock, link)

	def load_t2(self, query: dict[str, Any], for_update: bool=False) -> Generator[T2Document]:
		"""
		Load T2 documents from database
								  |  for_update  |
		condition                 | true | false |
		--------------------------+------+-------+
		not in db                 | item | item  |
		in db, code OK            | -    | db    |
		in db, code in code_match | item | item  |
		in db, other code         | item | item  |
		"""

		if self._current_item is not None:
			if for_update:
				# want to run dependent t2, but already resolved
				if self.col.count_documents({"code": DocumentCode.OK} | query):
					return
				for doc in self._current_item["t2"]:
					if doc["code"] != DocumentCode.OK and filter_applies(query, doc):
						# prevent this doc from being returned by a call with for_update=False
						if "_id" not in doc:
							doc["_id"] = ObjectId()  # type: ignore[typeddict-unknown-key]
						yield doc
			else:
				count = 0
				for count, doc in enumerate(super().load_t2(query, for_update), 1):  # noqa: B007
					yield doc
				if count == 0:
					# return t2 docs that are not in the database
					for doc in self._current_item["t2"]:
						if filter_applies(query, doc):
							yield doc
		else:
			raise RuntimeError("load_t2 called outside of consume loop")
