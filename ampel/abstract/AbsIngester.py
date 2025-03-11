from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from typing import Any

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.core.ContextUnit import ContextUnit
from ampel.protocol.LoggerProtocol import LoggerProtocol
from ampel.types import Traceless


class AbsIngester(AmpelABC, ContextUnit, abstract=True):
    error_callback: Traceless[None | Callable[[], None]] = None
    acknowledge_callback: Traceless[None | Callable[[Iterator[Any]], None]] = None

    raise_exc: bool = False

    run_id: Traceless[int]
    logger: Traceless[LoggerProtocol]

    @contextmanager
    @abstractmethod
    def group(self) -> Generator:
        """
        Ensure that documents ingested in this context are grouped together
        """
        ...

    @abstractmethod
    def acknowledge_on_delivery(self, message: Any) -> None:
        """
        Add a message to be passed to acknowledge_callback when the current
        batch of documents has been delivered
        """
        ...

    @abstractmethod
    def flush(self) -> None:
        """
        Wait for all documents to be stored
        """
        ...

    @property
    @abstractmethod
    def stock(self) -> AbsDocIngester[StockDocument]: ...

    @property
    @abstractmethod
    def t0(self) -> AbsDocIngester[DataPoint]: ...

    @property
    @abstractmethod
    def t1(self) -> AbsDocIngester[T1Document]: ...

    @property
    @abstractmethod
    def t2(self) -> AbsDocIngester[T2Document]: ...
