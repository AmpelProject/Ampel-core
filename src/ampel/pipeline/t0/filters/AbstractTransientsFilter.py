from abc import ABC, abstractmethod
from ampel.pipeline.common.flags.TransientFlags import TransientFlags

class AbstractTransientsFilter(ABC):

	on_match_default_flags = TransientFlags(0)

	def set_log_record_flag(self, flag):
		self.log_record_flag = flag

	def set_on_match_default_flags(self, flags):
		self.on_match_default_flags = flags

	@abstractmethod
	def set_filter_parameters(self, cut_values):
		pass

	@abstractmethod
	def apply(self, transient_candidate):
		pass
