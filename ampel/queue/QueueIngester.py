from collections.abc import Generator, Iterable
from contextlib import contextmanager
from functools import partial
from typing import Any

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsIngester import AbsIngester
from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.model.UnitModel import UnitModel
from ampel.queue.AbsProducer import AbsProducer


class QueueIngester(AbsIngester):

    producer: UnitModel

    class QueueStockIngester(AbsDocIngester[StockDocument]):
        queue: "QueueIngester"

        def ingest(self, doc: StockDocument) -> None:
            self.queue.add_stock(doc)

    class QueueT0Ingester(AbsDocIngester[DataPoint]):
        queue: "QueueIngester"

        def ingest(self, doc: DataPoint) -> None:
            self.queue.add_t0(doc)

    class QueueT1Ingester(AbsDocIngester[T1Document]):
        queue: "QueueIngester"

        def ingest(self, doc: T1Document) -> None:
            self.queue.add_t1(doc)

    class QueueT2Ingester(AbsDocIngester[T2Document]):
        queue: "QueueIngester"

        def ingest(self, doc: T2Document) -> None:
            self.queue.add_t2(doc)

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self._producer = self.context.loader.new(
            self.producer, unit_type=AbsProducer
        )

        self._stock = self.QueueStockIngester(queue=self)
        self._t0 = self.QueueT0Ingester(queue=self)
        self._t1 = self.QueueT1Ingester(queue=self)
        self._t2 = self.QueueT2Ingester(queue=self)

        self._item = AbsProducer.Item.new()

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

    def flush(self) -> None:
        self._producer.flush()

    def add_stock(self, doc: StockDocument) -> None:
        self._item.stock.append(doc)

    def add_t0(self, doc: DataPoint) -> None:
        self._item.t0.append(doc)

    def add_t1(self, doc: T1Document) -> None:
        self._item.t1.append(doc)

    def add_t2(self, doc: T2Document) -> None:
        self._item.t2.append(doc)

    @property
    def stock(self) -> AbsDocIngester[StockDocument]:
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

