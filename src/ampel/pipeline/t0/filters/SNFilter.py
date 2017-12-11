from ampel.pipeline.t0.filters.AbstractTransientsFilter import AbstractTransientsFilter

class SNFilter(AbstractTransientsFilter):

	def set_filter_parameters(self, d):
		self.parameters = d

	def apply(self, ampel_alert):
		for el in ampel_alert.get_photopoints():
			if el['magpsf'] < 18:
				return self.on_match_default_flags
		return None
