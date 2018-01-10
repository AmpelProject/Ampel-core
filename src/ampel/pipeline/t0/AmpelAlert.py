#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/AmpelAlert.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 10.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.flags.AlertFlags import AlertFlags
from werkzeug.datastructures import ImmutableDict, ImmutableList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader

class AmpelAlert:
	"""	
		T0 base class containing a read-only list of read-only photopoint dictionaries.
		(read-only convertion occurs in constructor).
		During pipeline processing, an alert is loaded and its content used to instanciate this class. 
		Then, the AmpelAlert instance is fed to every active T0 filter.
	"""	

	__isfrozen = False
	flags = AlertFlags.NO_FLAG


	@staticmethod
	def load_ztf_alert(arg):
		"""	
			Convenience method. 
		"""
		return AmpelAlert(*ZIAlertLoader.get_flat_pps_list_from_file(arg))


	@classmethod
	def add_class_flags(cls, arg_flags):
		"""
			Set alert flags (ampel.flags.AlertFlags) of this alert.
			Typically: observing instrument, alert issuer.
			For example: AlertFlags.INST_ZTF | AlertFlags.SRC_IPAC
		"""
		cls.flags |= arg_flags


	@classmethod
	def set_alert_keywords(cls, alert_keywords):
		"""
			Set using ampel config values.
			For ZTF IPAC alerts:
			keywords = {
                "transient_id" : "alertid",
                "photopoint_id" : "candid",
                "obs_date" : "jd",
                "filter_id" : "fid",
                "mag" : "magpsf"
            }
		"""
		AmpelAlert.alert_keywords = alert_keywords



	@classmethod
	def has_flags(cls, arg_flags):
		"""
			ex: AmpelAlert.has_flags(AlertFlags.INST_ZTF)
		"""
		return arg_flags in cls.flags


	def __init__(self, tran_id, list_of_pps):
		""" 
			tran_id: the astronomical transient object ID, for ZTF IPAC alerts 'objId'
			list_of_pps: a flat list of photopoint dictionaries. 
			Ampel makes sure that each dictionary contains an alFlags key 
		"""
		self.tran_id = tran_id

		# TODO: remove "is not None" check for production 
		self.pps = ImmutableList(
			[ImmutableDict(el) for el in list_of_pps if 'candid' in el and el['candid'] is not None]
		)
		self.__isfrozen = True


	def __setattr__(self, key, value):
		if self.__isfrozen:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)


	def get_values(self, param_name):
		"""
			ex: instance.get_values("mag")
		"""

		if param_name in AmpelAlert.alert_keywords:
			param_name = AmpelAlert.alert_keywords[param_name]
		
		return [
			el[param_name] 
			for el in self.pps if param_name in el
		]


	def get_tuples(self, param1, param2):
		"""
			ex: instance.get_tuples("obs_date", "mag")
		"""
		if param1 in AmpelAlert.alert_keywords:
			param1 = AmpelAlert.alert_keywords[param1]

		if param2 in AmpelAlert.alert_keywords:
			param2 = AmpelAlert.alert_keywords[param2]

		return [
			(el[param1], el[param2]) 
			for el in self.pps if param1 in el and param2 in el
		]


	def get_photopoints(self):
		"""
			returns a list of dicts
		"""
		return self.pps


	def get_id(self):
		"""
			returns the transient Id (ZTF: objectId)
		"""
		return self.tran_id
