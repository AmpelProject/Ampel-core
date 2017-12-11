from ampel.pipeline import AbstractTransientsFilter
from random import randint

class NoFilter(AbstractTransientsFilter):

	def __init__(self):
		return

	def set_cut_values(self, arg):
		return

	def passes(self, transient_candidate):
		return True
