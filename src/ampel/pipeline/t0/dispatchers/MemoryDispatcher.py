from ampel.pipeline import AbtractTransientsDispatcher
from ampel import TransientFlags


class MemoryDispatcher(AbtractTransientsDispatcher):
	"""
		Dispatcher class called by t0.AlertProcessor.
		This class is intended to be used for developing and testing filters.
		It stores transient candidates into accepted and rejected array 
		based on the configured filter outcomes.
	"""
	def __init__(self):
		self.accepted_transients = []
		self.rejected_transients = []
		self.accepted_transient_ids = []
		self.rejected_transient_ids = []

	def set_channel_flag(self, f):
		self.channel_flag = f

	def dispatch(self, ztf_alert, flags):

		if flags[0] is not None and TransientFlags.T0_MATCH in flags[0]:
			self.accepted_transients.append(ztf_alert)
			self.accepted_transient_ids.append(ztf_alert['candid'])
		else:
			self.rejected_transients.append(ztf_alert)
			self.rejected_transient_ids.append(ztf_alert['candid'])

	def get_accepted(self):
		return self.accepted_transients;

	def get_rejected(self):
		return self.rejected_transients;

	def get_accepted_ids(self):
		return self.accepted_transient_ids;

	def get_rejected_ids(self):
		return self.rejected_transient_ids;

