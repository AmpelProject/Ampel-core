from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from functools import partial
from typing import Any, Literal, Self

from bson import ObjectId

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsIngester import AbsIngester
from ampel.content.DataPoint import DataPoint
from ampel.content.JournalRecord import JournalRecord
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.MongoStockUpdater import BaseStockUpdater
from ampel.protocol.StockIngesterProtocol import StockIngesterProtocol
from ampel.queue.AbsProducer import AbsProducer
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.types import ChannelId, StockId, Tag

# ruff: noqa: SLF001

class QueueIngester(AbsIngester):
    producer: UnitModel

    class QueueStockUpdater(BaseStockUpdater):
        def __init__(
            self,
            queue: "QueueIngester",
            tier: Literal[-1, 0, 1, 2, 3],
            run_id: int,
            process_name: str,
            extra_tag: None | Tag | Sequence[Tag] = None,
            bump_updated: bool = True,

        ) -> None:
            super().__init__(
                tier=tier, run_id=run_id, process_name=process_name, extra_tag=extra_tag
            )
            self.queue = queue
            self.bump_updated = bump_updated


        def add_journal_record(
            self,
            stock: StockId | Sequence[StockId],
            jattrs: None | JournalAttributes = None,
            tag: None | Tag | Sequence[Tag] = None,
            name: None | str | Sequence[str] = None,
            trace_id: None | dict[str, int] = None,
            action_code: None | JournalActionCode = None,
            doc_id: None | ObjectId = None,
            unit: None | int | str = None,
            channel: None | ChannelId | Sequence[ChannelId] = None,
            now: None | int | float = None,
        ) -> JournalRecord:
            """
            Add a journal record to the stock document(s) identified by the input stock id(s)
            """
            jrec = self.new_journal_record(
                unit=unit,
                channels=channel,
                action_code=action_code,
                doc_id=doc_id,
                jattrs=jattrs,
                trace_id=trace_id,
                now=now,
            )

            names = {name} if isinstance(name, str) else set(name) if name else None
            tags = {tag} if isinstance(tag, Tag) else set(tag) if tag else None
            channels = [channel] if isinstance(channel, ChannelId) else channel if channel else []

            for doc in self._match_stocks(
                {stock} if isinstance(stock, StockId) else set(stock)
            ):
                doc["journal"] = [*doc.get("journal", []), jrec]
                if names:
                    doc["name"] = list(names.union(doc.get("name", [])))
                if tags:
                    doc["tag"] = list(tags.union(doc.get("tag", [])))
                
                if self.bump_updated:
                    for chan in ("any", *channels):
                        self._update_ts(doc, chan, jrec["ts"])

            return jrec

        @staticmethod
        def _update_ts(doc: StockDocument, channel: ChannelId, ts: int | float) -> None:
            if channel in doc["ts"]:
                if "upd" in doc["ts"][channel]:
                    doc["ts"][channel]["upd"] = max(ts, doc["ts"][channel]["upd"])
                else:
                    doc["ts"][channel]["upd"] = ts
            else:
                doc["ts"][channel] = {"upd": ts}

        def add_name(self, stock: StockId, name: str | Sequence[str]) -> None:
            names = {name} if isinstance(name, str) else set(name)
            for doc in self._match_stocks({stock}):
                doc["name"] = list(names.union(doc.get("name", [])))

        def add_tag(
            self, stock: StockId | Sequence[StockId], tag: Tag | Sequence[Tag]
        ) -> None:
            tags = {tag} if isinstance(tag, Tag) else set(tag)
            for doc in self._match_stocks(
                {stock} if isinstance(stock, StockId) else set(stock)
            ):
                doc["tag"] = list(tags.union(doc.get("tag", [])))

        def _match_stocks(
            self, stock_ids: set[StockId]
        ) -> Generator[StockDocument, None, None]:
            for stock in self.queue._item.stock:
                if stock["stock"] in stock_ids:
                    yield stock

    class QueueStockIngester(AbsDocIngester[StockDocument]):
        queue: "QueueIngester"
        update: "QueueIngester.QueueStockUpdater"

        def ingest(self, doc: StockDocument) -> None:
            self.queue._add_stock(doc)

    class QueueT0Ingester(AbsDocIngester[DataPoint]):
        queue: "QueueIngester"

        def ingest(self, doc: DataPoint) -> None:
            self.queue._add_t0(doc)

    class QueueT1Ingester(AbsDocIngester[T1Document]):
        queue: "QueueIngester"

        def ingest(self, doc: T1Document) -> None:
            self.queue._add_t1(doc)

    class QueueT2Ingester(AbsDocIngester[T2Document]):
        queue: "QueueIngester"

        def ingest(self, doc: T2Document) -> None:
            self.queue._add_t2(doc)

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self._producer = self.context.loader.new(self.producer, unit_type=AbsProducer)

        self._stock = self.QueueStockIngester(
            queue=self,
            update=self.QueueStockUpdater(
                queue=self,
                tier=self.tier,
                run_id=self.run_id,
                process_name=self.process_name,
            ),
        )
        self._t0 = self.QueueT0Ingester(queue=self)
        self._t1 = self.QueueT1Ingester(queue=self)
        self._t2 = self.QueueT2Ingester(queue=self)

        self._item = AbsProducer.Item.new()

    def __enter__(self) -> "Self":
        self._producer.__enter__()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback) -> bool | None:
        return self._producer.__exit__(exc_type, exc_value, traceback)

    @contextmanager
    def group(self, acknowledge_messages: None | Iterable[Any] = None) -> Generator:
        """
        Ensure that updates issued in this context are grouped together
        """
        yield
        item = self._swap_buffer()
        if item or acknowledge_messages:
            self._producer.produce(
                item,
                partial(self.acknowledge_callback, acknowledge_messages)
                if self.acknowledge_callback and acknowledge_messages is not None
                else None,
            )

    def _swap_buffer(self) -> AbsProducer.Item:
        prev = self._item
        self._item = AbsProducer.Item.new()
        return prev

    def _add_stock(self, doc: StockDocument) -> None:
        self._item.stock.append(doc)

    def _add_t0(self, doc: DataPoint) -> None:
        self._item.t0.append(doc)

    def _add_t1(self, doc: T1Document) -> None:
        self._item.t1.append(doc)

    def _add_t2(self, doc: T2Document) -> None:
        self._item.t2.append(doc)

    @property
    def stock(self) -> StockIngesterProtocol:
        return self._stock

    @property
    def t0(self) -> AbsDocIngester[DataPoint]:
        return self._t0

    @property
    def t1(self) -> AbsDocIngester[T1Document]:
        return self._t1

    @property
    def t2(self) -> AbsDocIngester[T2Document]:
        return self._t2
