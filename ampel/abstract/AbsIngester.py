from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from typing import Any, Literal

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.core.ContextUnit import ContextUnit
from ampel.log.AmpelLogger import AmpelLogger
from ampel.protocol.StockIngesterProtocol import StockIngesterProtocol
from ampel.types import Traceless


class AbsIngester(AmpelABC, ContextUnit, abstract=True):
    error_callback: Traceless[None | Callable[[], None]] = None
    acknowledge_callback: Traceless[None | Callable[[Iterable[Any]], None]] = None

    raise_exc: bool = False

    run_id: Traceless[int]
    tier: Traceless[Literal[-1, 0, 1, 2, 3]]
    process_name: Traceless[str]
    logger: Traceless[AmpelLogger]

    @contextmanager
    @abstractmethod
    def group(self, acknowledge_messages: None | Iterable[Any] = None) -> Generator:
        """
        Ensure that documents ingested in this context are grouped together

        :param acknowledge_messages: messages to be passed to
            acknowledge_callback when documents ingested in the context are
            delivered
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
    def stock(self) -> StockIngesterProtocol: ...

    @property
    @abstractmethod
    def t0(self) -> AbsDocIngester[DataPoint]: ...

    @property
    @abstractmethod
    def t1(self) -> AbsDocIngester[T1Document]: ...

    @property
    @abstractmethod
    def t2(self) -> AbsDocIngester[T2Document]: ...
