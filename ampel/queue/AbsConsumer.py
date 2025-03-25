from collections.abc import Iterable
from typing import Generic

from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.base.AmpelUnit import AmpelUnit
from ampel.base.decorator import abstractmethod
from ampel.types import (
	T,
)


class AbsConsumer(AbsContextManager, AmpelUnit, Generic[T], abstract=True):

	@abstractmethod
	def consume(self) -> None | T:
		"""Get a single message from the queue"""
		...
	
	@abstractmethod
	def acknowledge(self, docs: Iterable[T]) -> None:
		"""Acknowledge the processing of a batch of messages"""
		...