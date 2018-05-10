#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AmpelAlert.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlertFlags import AlertFlags
import operator

class AmpelAlert:
	"""
	T0 base class containing a read-only list of read-only photopoint dictionaries.
	(read-only convertion occurs in constructor).
	During pipeline processing, an alert is loaded and its content used to instantiate this class. 
	Then, the AmpelAlert instance is fed to every active T0 filter.
	"""

	flags = AlertFlags.NO_FLAG
	alert_keywords = {}
	alert_kws_set = set()

	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'==': operator.eq,
		'!=': operator.ne,
		'is': operator.is_,
		'is not': operator.is_not
	}


	@staticmethod
	def load_ztf_alert(arg):
		"""	
		Convenience method.
		Do not use for production!
		"""

		from ampel.pipeline.t0.alerts.AvroDeserializer import AvroDeserializer
		from ampel.pipeline.t0.alerts.ZIAlertParser import ZIAlertParser

		parsed_alert = ZIAlertParser().shape(
    		AvroDeserializer.load_raw_dict_from_file(arg)
		)

		return AmpelAlert(
			parsed_alert['tran_id'], 
			parsed_alert['ro_pps'], 
			parsed_alert['ro_uls']
		)


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
		cls.alert_keywords = alert_keywords
		cls.alert_kws_set = set(alert_keywords.keys())


	@classmethod
	def has_flags(cls, arg_flags):
		"""
			ex: AmpelAlert.has_flags(AlertFlags.INST_ZTF)
		"""
		return arg_flags in cls.flags


	def __init__(self, tran_id, list_of_pps, list_of_uls=None):
		""" 
		AmpelAlert constructor
		Parameters:
		alertid: unique identifier of the alert (for ZTF: candid of most recent photopoint)
		tran_id: the astronomical transient object ID, for ZTF IPAC alerts 'objectId'
		list_of_pps: a flat list of photopoint dictionaries. 
		Ampel makes sure that each dictionary contains an alFlags key 
		"""
		self.tran_id = tran_id
		self.pps = list_of_pps
		self.uls = list_of_uls

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


	def get_values(self, param_name, filters=None, upper_limits=False):
		"""
		ex: instance.get_values("mag")
		"""

		if param_name in AmpelAlert.alert_keywords:
			param_name = AmpelAlert.alert_keywords[param_name]

		if filters is None:
			browse_pps = self.uls if upper_limits else self.pps
		else:
			browse_pps = self.apply_filter(self.uls, filters) if upper_limits else self.apply_filter(self.pps, filters)

		return tuple(
			el[param_name] 
			for el in browse_pps if param_name in el
		)


	def apply_filter(self, match_pps, filters):
		"""
		"""

		if type(filters) is dict:
			filters = [filters]

		for filter_el in filters:

			operator = AmpelAlert.ops[
				filter_el['operator']
			]

			provided_field = filter_el['attribute']
			filter_el['value']

			attr_name = (
				provided_field if not provided_field in AmpelAlert.alert_keywords 
				else AmpelAlert.alert_keywords[provided_field]
			)

			match_pps = tuple(
				filter(
					lambda el: attr_name in el and operator(el[attr_name], filter_el['value']), 
					match_pps
				)
			)

		return match_pps


	def get_tuples(self, param1, param2, filters=None, upper_limits=False):
		"""
		ex: instance.get_tuples("obs_date", "mag")
		"""
		if param1 in AmpelAlert.alert_keywords:
			param1 = AmpelAlert.alert_keywords[param1]

		if param2 in AmpelAlert.alert_keywords:
			param2 = AmpelAlert.alert_keywords[param2]

		if filters is None:
			browse_pps = self.uls if upper_limits else self.pps
		else:
			browse_pps = self.apply_filter(self.uls, filters) if upper_limits else self.apply_filter(self.pps, filters)

		return tuple(
			(el[param1], el[param2]) 
			for el in browse_pps if param1 in el and param2 in el
		)


	def get_ntuples(self, params, filters=None, upper_limits=False):
		"""
		params: list of strings
		ex: instance.get_ntuples(["fid", "obs_date", "mag"])
		"""

		# If any of the provided parameter matches defined keyword mappings
		if AmpelAlert.alert_kws_set & set(params):
			for i, param in enumerate(params):
				if param in AmpelAlert.alert_keywords:
					params[i] = AmpelAlert.alert_keywords[param]
	
		# Filter photopoints if filter was provided
		if filters is None:
			browse_pps = self.uls if upper_limits else self.pps
		else:
			browse_pps = self.apply_filter(self.uls, filters) if upper_limits else self.apply_filter(self.pps, filters)

		return tuple(
			tuple(el[param] for param in params) 
			for el in browse_pps if all(param in el for param in params)
		)


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
