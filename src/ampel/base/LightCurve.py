#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/LightCurve.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from werkzeug.datastructures import ImmutableList
import operator, logging

class LightCurve:
	"""
		Class containing a collection of PhotoPoint instances.
		And a few convenience methods to return values embedded in the collection.
	"""
	
	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'=': operator.eq,
		'in': operator.contains
	}


	def __init__(self, pps_obj_list, read_only=True, logger=None):
		"""
			pps_obj_list: list of ampel.pipeline.common.PhotoPoint instances
			read_only: wether the provided list should be casted as ImmutableList 
			and the class instance frozen
		"""

		if read_only:
			self.pps_obj_list = ImmutableList(pps_obj_list)
			self.is_frozen = True
		else:
			self.pps_obj_list = pps_obj_list

		if logger is not None:
			logger.info("LightCurve loaded with " + str(len(self.pps_obj_list)) + " PhotoPoints")


	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_LightCurve__isfrozen' and not simply '__isfrozen' due to 'Private name mangling'
		if getattr(self, "_LightCurve__isfrozen", None) is not None:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)


	def filter_pps(self, filters):
		"""
		"""

		filtered_pps = []

		if type(filters) is dict:
			filters = [filters]

		for filter_el in filters:

			operator = LightCurve.ops[filter_el['op']] 
			del filter_el['op']

			for fkey in filter_el.keys():
				filtered_pps = list(
					filter(
						lambda pp: pp.has_parameter(fkey) and operator(pp.get_value(fkey), filter_el[fkey]), 
						self.pps_obj_list
					)
				)

		return filtered_pps


	def get_values(self, field_name, filters=None):
		"""
			ex: instance.get_values('obs_date')
		"""

		if filters is not None and type(filters) is not dict and type(filters) is not list:
			raise ValueError("filters must be of type dict or list")
			
		pps = self.pps_obj_list if filters is None else self.filter_pps(filters)

		return [
			pp.get_value(field_name) 
			for pp in pps if pp.has_parameter(field_name)
		]


	def get_tuples(self, field1_name, field2_name, filters=None):
		"""
			ex: instance.get_values('obs_date', 'mag')
		"""

		if filters is not None and type(filters) is not dict and type(filters) is not list:
			raise ValueError("filters must be of type dict or list")

		pps = self.filter_pps(filters) if filters is not None else self.pps_obj_list
		return [
			(pp.get_value(field1_name), pp.get_value(field2_name))
			for pp in pps if pp.has_parameter(field1_name) and pp.has_parameter(field2_name)
		]

	
	def get_photopoints(self, filters=None):
		"""
		"""
		return self.filter_pps(filters) if filters is not None else self.pps_obj_list


	def get_pos(self, ret="mean", filters=None):
		"""
		ret (for all methods, only matching PhotoPoint wrt the provided filter(s) are used!):
			"raw": returns ((ra, dec), (ra, dec), ...)  
			"mean": returns (<ra>, <dec>) 
			"brightest": returns (ra, dec) 
			"latest": returns (ra, dec) 

		examples: 
		instance.get_pos("brightest", {'alFlags': PhotoPointFlags.ZTF_G, 'in'})
			returns the position of the brightest PhotoPoint in the ZTF G band

		instance.get_pos("lastest", {'magpsf': 18, '<'})
			returns the position of the latest PhotoPoint in time with a magnitude brighter than 18
			(or an empty array if no PhotoPoint matches this criteria)
		"""

		if ret == "raw": 
			return self.get_tuples("ra", "dec", filters=filters)

		pps = self.filter_pps(filters) if filters is not None else self.pps_obj_list

		if ret == "mean": 
			ras = [pp.get_value("ra") for pp in pps]
			decs = [pp.get_value("dec") for pp in pps]
			return (ras/len(ras), decs/len(decs))
		elif ret == "brightest": 
			mags = pps.copy()
			mags.sort(key=lambda x: x.get_value('magpsf'))
			return (mags[-1].get_value('ra'), mags[-1].get_value('dec'))
		elif ret == "latest": 
			mags = pps.copy()
			mags.sort(key=lambda x: x.get_value('obs_date'))
			return (mags[-1].get_value('ra'), mags[-1].get_value('dec'))
		else:
			raise NotImplementedError("ret method: " + ret + " is not implemented")
