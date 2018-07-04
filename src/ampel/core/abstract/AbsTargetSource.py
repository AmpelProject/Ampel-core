
from ampel.base.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsTargetSource(metaclass=AmpelABC):
	"""
	Provides target fields for follow-up searches
	"""
	
	@abstractmethod
	async def get_targets(self):
	
		"""
		:yields: a tuple ((ra,dec), radius, (date_min, date_max), [channels])
		"""
		pass
