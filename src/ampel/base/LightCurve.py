#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/LightCurve.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 04.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import operator

class LightCurve:
	"""
	Class containing a collection of PhotoPoint/UpperLimit instances.
	And a few convenience methods to return values embedded in the collection.

	This object has many similarities with AmpelAlert and yet some notable differences.
	I wish I had the time to think of a possible object oriented parent/child 
	structure for these two classes (AmpelAlert efficiency is by the way 
	an important criteria since *every* ZTF alert instanciates an AmpelAlert obj).
	"""
	
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

	
	def __init__(self, compound_dict, ppo_list, ulo_list=None, read_only=True, logger=None):
		"""
		compound: dict instance loaded using compound DB dict
		ppo_list: list of ampel.base.PhotoPoint instances
		ulo_list: list of ampel.base.UpperLimit instances
		read_only: wether the provided list should be casted as immutable tuple
		and the class instance frozen
		"""

		self.id = compound_dict['_id']
		self.tier = compound_dict['tier']
		self.added = compound_dict['added']
		self.lastppdt = compound_dict['lastppdt']

		if read_only:
			self.ppo_list = tuple(el for el in ppo_list)
			self.ulo_list = tuple(el for el in ulo_list) if ulo_list is not None else []
			self.is_frozen = True
		else:
			self.ppo_list = ppo_list
			self.ulo_list = ulo_list if ulo_list is not None else []

		if logger is not None:
			logger.info(
				"LightCurve loaded with %i photopoints and %i upper limits" % 
				(len(self.ppo_list), len(self.ulo_list))
			)

	
	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_LightCurve__isfrozen' and not simply '__isfrozen' due to 'Private name mangling'
		if getattr(self, "_LightCurve__isfrozen", None) is not None:
			raise TypeError("%r is a frozen instance " % self)

		object.__setattr__(self, key, value)


	def apply_filter(self, match_objs, filters):
		"""
		"""
		if type(filters) is dict:
			filters = [filters]
		else:
			if filters is None or type(filters) is not list:
				raise ValueError("filters must be of type dict or list")

		for filter_el in filters:

			operator = LightCurve.ops[
				filter_el['operator']
			]

			attr_name = filter_el['attribute']
			match_objs = tuple(
				filter(
					lambda x: x.has_parameter(attr_name) and operator(x.get_value(attr_name), filter_el['value']), 
					match_objs
				)
			)

		return match_objs


	def get_values(self, field_name, filters=None, upper_limits=False):
		"""
		ex: instance.get_values('obs_date')
		'filters' example: {'attribute': 'magpsf', 'operator': '<', 'value': 18}
		'upper_limits': if set to True, upper limits are returned instead of photopoints
		"""
		return [
			obj.get_value(field_name) 
			for obj in self._get_photo_objs(filters, upper_limits) 
			if obj.has_parameter(field_name)
		]


	def get_tuples(self, field1_name, field2_name, filters=None, upper_limits=False):
		"""
		ex: instance.get_values('obs_date', 'mag')
		'filters' example: {'attribute': 'magpsf', 'operator': '<', 'value': 18}
		'upper_limits': if set to True, upper limits are returned instead of photopoints
		"""
		return [
			(obj.get_value(field1_name), obj.get_value(field2_name))
			for obj in self._get_photo_objs(filters, upper_limits) 
			if obj.has_parameter(field1_name) and obj.has_parameter(field2_name)
		]


	def get_ntuples(self, params, filters=None, upper_limits=False):
		"""
		params: list of strings
		ex: instance.get_ntuples(["fid", "obs_date", "mag"])
		'filters' example: {'attribute': 'magpsf', 'operator': '<', 'value': 18}
		'upper_limits': if set to True, upper limits are returned instead of photopoints
		"""
		return tuple(
			tuple(obj[param] for param in params) 
			for obj in self._get_photo_objs(filters, upper_limits) 
			if all(obj.has_parameter(param) for param in params)
		)

	
	def get_photopoints(self, filters=None):
		""" returns a list of dicts """
		return (
			self.apply_filter(self.ppo_list, filters) if filters is not None 
			else self.ppo_list
		)


	def get_upperlimits(self, filters=None):
		""" returns a list of dicts """
		return (
			self.apply_filter(self.ulo_list, filters) if filters is not None 
			else self.ulo_list
		)


	def _get_photo_objs(self, filters, upper_limits):
		"""	
		"""	
		if filters is None:
			return self.ulo_list if upper_limits else self.ppo_list
		else:
			return (
				self.apply_filter(self.ulo_list, filters) if upper_limits 
				else self.apply_filter(self.ppo_list, filters)
			)


	def get_pos(self, ret="brightest", filters=None):
		"""
		ret (for all methods, only matching PhotoPoint wrt the provided filter(s) are used!):
			"raw": returns ((ra, dec), (ra, dec), ...)
			"mean": returns (<ra>, <dec>)
			"brightest": returns (ra, dec)
			"latest": returns (ra, dec)

		examples: 
		instance.get_pos("brightest", {'alFlags': PhotoFlags.ZTF_G, 'in'})
			returns the position of the brightest PhotoPoint in the ZTF G band

		instance.get_pos("lastest", {'attribute': 'magpsf', 'operator': '<', 'value': 18})
			returns the position of the latest PhotoPoint in time with a magnitude brighter than 18
			(or an empty array if no PhotoPoint matches this criteria)
		"""

		if ret == "raw": 
			return self.get_tuples("ra", "dec", filters=filters)

		pps = self.apply_filter(self.ppo_list, filters) if filters is not None else self.ppo_list

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
			raise NotImplementedError("ret method: %s is not implemented" % ret)
