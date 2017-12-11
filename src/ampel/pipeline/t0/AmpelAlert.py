from ampel.pipeline.t0.AlertFlags import AlertFlags
from werkzeug.datastructures import ImmutableDict, ImmutableList


class AmpelAlert:
	"""	 
		T0 base class containing a read-only list of read-only photopoints dictionaries.
		The read-only conversion occurs in the contructor.
		Typically, during pipeline processing, an alert is loaded and used to instanciate this class. 
		Then, the AmpelAlert instance is fed to every active T0 filter.
	"""	 

	__isfrozen = False
	alert_flags = AlertFlags.NO_FLAG


	@staticmethod
	def load_ztf_alert(arg):
		"""	 
			Convenience method. 
			The import statement impacts performance, don't use this method in a loop 
		"""
		from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
		return AmpelAlert(*ZIAlertLoader.get_flat_pps_list_from_file(arg))


	@classmethod
	def add_alert_flags(cls, arg_flags):
		"""
			Set alert flags (t0.AlertFlags) of this alert.
			Typically: observing instrument, photopoints source and alert issuer.
			For example: AlertFlags.INST_ZTF | AlertFlags.PP_IPAC | AlertFlags.ALERT_IPAC 
		"""
		cls.alert_flags |= arg_flags


	@classmethod
	def set_pp_dict_keywords(cls, keywords):
		"""
		"""
		AmpelAlert.tran_id = keywords["tranId"]
		AmpelAlert.pp_id = keywords["pptId"]
		AmpelAlert.obs_date = keywords["obsDate"]
		AmpelAlert.filter_id = keywords["filterId"]


	@classmethod
	def set_pp_dict_keyword(cls, arg, val):
		setattr(AmpelAlert, arg, val)


	@classmethod
	def has_flags(cls, arg_flags):
		return arg_flags in cls.alert_flags


	def __init__(self, tran_id, list_of_pps_dicts):
		""" 
			tran_id: the astronomical transient object ID, for ZTF IPAC alerts 'objId'
			list_of_pps_dicts: a flat list of dictionaries. 
			Ampel makes sure that each dictionary contains an alflags key 
		"""
		self.tran_id = tran_id

		# TODO: remove is not None for production (should not happen)
		self.pps = ImmutableList(
			[ImmutableDict(el) for el in list_of_pps_dicts if 'candid' in el and el['candid'] is not None]
		)
		self.__isfrozen = True


	def __setattr__(self, key, value):
		if self.__isfrozen:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)


	def get_subres_ids(self, ignore_none=False):
		return self.get_parameter(AmpelAlert.pp_id, ignore_none=True)


	def get_parameter(self, param_name, ignore_none=False):
		return [el[param_name] for el in self.pps if param_name in el]


	def get_tupple(self, p1, p2):
		return [[el[p1], el[p2]] for el in self.pps if p1 in el and p2 in el]


	def plot_tupple(self, p1, p2):
		import matplotlib.pyplot as plt
		plt.scatter(*zip(*self.get_tupple(p1, p2)))
		plt.xlabel(p1)
		plt.ylabel(p2)
		plt.grid(True)
		plt.show()


	def get_photopoints(self):
		return self.pps


	def get_id(self):
		return self.tran_id
