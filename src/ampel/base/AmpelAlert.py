#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/AmpelAlert.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 12.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.Frozen import Frozen
from ampel.flags.AlertFlags import AlertFlags
import operator

class AmpelAlert(Frozen):
	"""
	T0 base class containing a read-only list of read-only photopoint dictionaries.
	(read-only convertion occurs in constructor).
	During pipeline processing, an alert is loaded and its content used to instantiate this class. 
	Then, the AmpelAlert instance is fed to every active T0 filter.
	"""

	flags = AlertFlags(0)
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
	def load_ztf_alert(arg, cutout=True):
		"""	
		Convenience method.
		Do not use for production!
		"""
		import fastavro
		with open(arg, "rb") as fo:
			al = next(fastavro.reader(fo), None)

		if al.get('prv_candidates') is None:
			return AmpelAlert(
				al['objectId'], [al['candidate']], None, 
				al.get('cutoutScience') if cutout else None
			)
		else:
			pps = [d for d in al['prv_candidates'] if d.get('candid') is not None]
			pps.insert(0,  al['candidate'])
			return AmpelAlert(
				al['objectId'], pps, 
				[d for d in al['prv_candidates'] if d.get('candid') is None],
				al.get('cutoutScience') if cutout else None
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


	def __init__(self, tran_id, list_of_pps, list_of_uls=None, cutout=None):
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
		self.cutout = cutout

		# Freeze this instance
		self.__isfrozen = True


	def get_values(self, param_name, filters=None, upper_limits=False):
		"""
		ex: instance.get_values("mag")
		"""
		if param_name in AmpelAlert.alert_keywords:
			param_name = AmpelAlert.alert_keywords[param_name]

		return tuple(
			el[param_name] 
			for el in self._get_photo_objs(filters, upper_limits) if param_name in el
		)


	def get_tuples(self, param1, param2, filters=None, upper_limits=False):
		"""
		ex: instance.get_tuples("obs_date", "mag")
		"""
		if param1 in AmpelAlert.alert_keywords:
			param1 = AmpelAlert.alert_keywords[param1]

		if param2 in AmpelAlert.alert_keywords:
			param2 = AmpelAlert.alert_keywords[param2]

		return tuple(
			(el[param1], el[param2]) 
			for el in self._get_photo_objs(filters, upper_limits) if param1 in el and param2 in el
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
	
		return tuple(
			tuple(el[param] for param in params) 
			for el in self._get_photo_objs(filters, upper_limits) if all(param in el for param in params)
		)


	def _get_photo_objs(self, filters, upper_limits):
		
		# Filter photopoints if filter was provided
		if filters is None:
			return self.uls if upper_limits else self.pps
		else:
			return self.apply_filter(self.uls, filters) if upper_limits else self.apply_filter(self.pps, filters)


	def get_photopoints(self):
		""" returns a list of dicts """
		return self.pps


	def get_upperlimits(self):
		""" returns a list of dicts """
		return self.uls


	def get_id(self):
		"""
		returns the transient Id (ZTF: objectId)
		"""
		return self.tran_id


	def apply_filter(self, match_objs, filters):
		"""
		"""

		if type(filters) is dict:
			filters = [filters]
		else:
			if filters is None or type(filters) is not list:
				raise ValueError("filters must be of type dict or list")

		for filter_el in filters:

			operator = AmpelAlert.ops[
				filter_el['operator']
			]

			filter_attr_name = filter_el['attribute']
			attr_name = (
				filter_attr_name if not filter_attr_name in AmpelAlert.alert_keywords 
				else AmpelAlert.alert_keywords[filter_attr_name]
			)

			match_objs = tuple(
				filter(
					lambda el: attr_name in el and operator(el[attr_name], filter_el['value']), 
					match_objs
				)
			)

		return match_objs
