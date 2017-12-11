from abc import ABC, abstractmethod


class AbtractTransientsDispatcher(ABC):

	def __init__(self):
		return

	@abstractmethod
	def dispatch(self, transient_candidate):
		return
