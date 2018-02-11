#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AmpelAlert.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.flags.AlertFlags import AlertFlags
from werkzeug.datastructures import ImmutableDict, ImmutableList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
import operator

class AmpelAlert:
	"""	
		T0 base class containing a read-only list of read-only photopoint dictionaries.
		(read-only convertion occurs in constructor).
		During pipeline processing, an alert is loaded and its content used to instanciate this class. 
		Then, the AmpelAlert instance is fed to every active T0 filter.
	"""	

	flags = AlertFlags.NO_FLAG

	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'=': operator.eq
	}

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
		Set using ampel config values. For ZTF IPAC alerts:
		alert_keywords = {
			"transient_id" : "objectId",
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
		AmpelAlert constructor
		Parameters:
		tran_id: the astronomical transient object ID, for ZTF IPAC alerts 'objId'
		list_of_pps: a flat list of photopoint dictionaries. 
		Ampel makes sure that each dictionary contains an alFlags key 
		"""
		self.tran_id = tran_id

		# TODO: remove "is not None" check for production 
		self.pps = ImmutableList(
			[ImmutableDict(el) for el in list_of_pps if 'candid' in el and el['candid'] is not None]
		)

		# Freeze this instance
		self.__isfrozen = True


	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_AmpelAlert__isfrozen' and not simply '__isfrozen' due to 'Private name mangling'
		if getattr(self, "_AmpelAlert__isfrozen", None) is not None:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)


	def get_values(self, param_name, filters=None):
		"""
		ex: instance.get_values("mag")
		"""

		if param_name in AmpelAlert.alert_keywords:
			param_name = AmpelAlert.alert_keywords[param_name]

		filtered_pps = self.pps if filters is None else self.filter_pps(filters)
	
		return [
			el[param_name] 
			for el in filtered_pps if param_name in el
		]


	def filter_pps(self, filters):
		"""
		"""
		filtered_pps = self.pps

		if type(filters) is dict:
			filters = [filters]

		for filter_el in filters:

			operator = AmpelAlert.ops[
				filter_el['operator']
			] 

			for fkey in filter_el.keys():

				if fkey == "operator":
					continue

				akey = fkey if not fkey in AmpelAlert.alert_keywords else AmpelAlert.alert_keywords[fkey]

				filtered_pps = list(
					filter(
						lambda el: akey in el and operator(el[akey], filter_el[fkey]), 
						filtered_pps
					)
				)

		return filtered_pps


	def get_tuples(self, param1, param2, filters=None):
		"""
		ex: instance.get_tuples("obs_date", "mag")
		"""
		if param1 in AmpelAlert.alert_keywords:
			param1 = AmpelAlert.alert_keywords[param1]

		if param2 in AmpelAlert.alert_keywords:
			param2 = AmpelAlert.alert_keywords[param2]

		filtered_pps = self.pps if filters is None else self.filter_pps(filters)

		return [
			(el[param1], el[param2]) 
			for el in filtered_pps if param1 in el and param2 in el
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
