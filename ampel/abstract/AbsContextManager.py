
from types import TracebackType
from typing import Self

from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod


class AbsContextManager(AmpelABC, abstract=True):
    """
    AmpelABC version of contextlib.AbstractContextManager
    """

    def __enter__(self) -> "Self":
        return self

    @abstractmethod
    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None | bool:
        ...
