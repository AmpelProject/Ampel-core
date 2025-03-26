from collections.abc import Iterable
from threading import Event
from typing import Generic

from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.base.AmpelUnit import AmpelUnit
from ampel.base.decorator import abstractmethod
from ampel.types import T, Traceless


class AbsConsumer(AbsContextManager, AmpelUnit, Generic[T], abstract=True):

	#: Event that can be set to break out of consume()
	stop: Traceless[Event]

	@abstractmethod
	def consume(self) -> None | T:
		"""Get a single message from the queue, returning None if the queue is empty, or stop is set"""
		...
	
	@abstractmethod
	def acknowledge(self, docs: Iterable[T]) -> None:
		"""Acknowledge the processing of a batch of messages"""
		...