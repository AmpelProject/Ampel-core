from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import partial
from typing import Any, NamedTuple, TypeVar

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelUnit import AmpelUnit
from ampel.base.decorator import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.model.UnitModel import UnitModel

from .AbsIngester import AbsIngester

_T = TypeVar("_T", StockDocument, DataPoint, T1Document, T2Document)


class AbsProducer(AmpelABC, AmpelUnit, abstract=True):

    class Item(NamedTuple):
        """
        A bundle of documents that all belong to the same context (e.g. an alert)
        """

        stock: list[StockDocument]
        t0: list[DataPoint]
        t1: list[T1Document]
        t2: list[T2Document]

        @classmethod
        def new(cls) -> "AbsProducer.Item":
            return cls([], [], [], [])

    @abstractmethod
    def produce(
        self, item: Item, delivery_callback: None | Callable[[], None]
    ) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...


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
        self._messages_to_ack: set[Any] = set()

    @contextmanager
    def group(self) -> Generator:
        """
        Ensure that updates issued in this context are grouped together
        """
        item = AbsProducer.Item.new()
        messages: set[Any] = set()
        prev = self._item, self._messages_to_ack
        self._item, self._messages = item, messages
        try:
            yield
        finally:
            self._item, self._messages_to_ack = prev
            self._producer.produce(
                item,
                partial(self.acknowledge_callback, iter(messages))
                if self.acknowledge_callback
                else None,
            )

    def acknowledge_on_delivery(self, message) -> None:
        self._messages_to_ack.add(message)

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

