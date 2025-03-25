from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.base.AmpelUnit import AmpelUnit
from ampel.base.decorator import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document

_T = TypeVar("_T", StockDocument, DataPoint, T1Document, T2Document)

class AbsProducer(AbsContextManager, AmpelUnit, abstract=True):

    @dataclass
    class Item:
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
        
        def __bool__(self) -> bool:
            return any((self.stock, self.t0, self.t1, self.t2))

    @abstractmethod
    def produce(
        self, item: Item, delivery_callback: None | Callable[[], None]
    ) -> None: ...
