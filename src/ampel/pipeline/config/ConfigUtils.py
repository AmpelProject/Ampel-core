#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ConfigUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.10.2018
# Last Modified Date: 17.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension

class ConfigUtils:


	@classmethod
	def has_nested_type(cls, obj, target_type, strict=True):
		"""
		:param obj: object instance (dict/list/set/tuple)
		:param type target_type: example: ReadOnlyDict/list
		"""

		if strict: 
			if type(obj) is target_type:
				return True
		else:
			if isinstance(obj, target_type):
				return True

		if isinstance(obj, dict):
			for el in obj.values():
				if cls.has_nested_type(el, target_type):
					return True

		elif AmpelUtils.is_sequence(obj):
			for el in obj:
				if cls.has_nested_type(el, target_type):
					return True

		return False


	@classmethod
	def check_flags_from_dict(cls, value, FlagClass, **kwargs):
		""" """
		if isinstance(value, BaseModel):
			arg = next(iter(value.dict().values()))
		else:
			arg = next(iter(value.values()))
		field_name = kwargs['field'].name.split("_")[0]

		if AmpelUtils.check_seq_inner_type(arg, str):
			cls.check_flags_from_seq(arg, field_name, FlagClass)
		else:
			for v in arg:  
				if isinstance(v, str):
					cls.check_flags_from_seq([v], field_name, FlagClass)
				elif isinstance(v, dict):
					cls.check_flags_from_seq(next(iter(v.values())), field_name, FlagClass)


	@classmethod
	def check_flags_from_seq(cls, flags, field_name, FlagClass):
		"""
		"""
		for el in AmpelUtils.iter(flags):

			if type(el) is str:
				try:
					# pylint: disable=unsubscriptable-object
					FlagClass[el]
				except KeyError:
					AmpelModelExtension.print_and_raise(
						header="transients->select->%s config error" % field_name,
						msg="Unknown flag '%s'.\nPlease check class %s for allowed flags" % 
							(el, FlagClass.__name__)
					)


	@classmethod
	def conditional_expr_converter(cls, arg, level=1):
		"""
		Converts JSON encoded conditional statements from Ampel config file 
		into arrays with dimention up to two.
		'anyOf' -> or operator -> encoded in a array elements of depth=1
		'allOf' -> and operator -> encoded in array elements of depth=2

		Accepted input: 
		---------------
		
		atomar values str, int float: "a" / 1 / 1.2
	
		1d sequences of atomar values (automaticallOfy treated as 'anyOf' sequence):
		[1, 2, 3]  / [1, "a", 3.4]
	
		'anyOf' dict containing 1d list of atomar values (explicit 'anyOf' sequence):
		{'anyOf': [1, 2, 3]} / {'anyOf': [1, "a", 3.4]}
	
		'allOf' dict containing 1d list of atomar values
		{'allOf': [1, 2, 3]} / {'anyOf': [1, "a", 3.4]}
	
		Nested structure whereby 'allOf' closes the nesting (can contain only a sequence of atomar values)
		{
			'anyOf': [
				{'allOf': ["HUSN1", "HUSN2"]}, 
				"HUBH1", 
				{'allOf': ["HUSN1", "HUSN3"]}
			]
		}
	
		Examples:
		---------
	
		```
		In []: conditional_expr_converter("abc")
		Out[]: 'abc'
	
		In []: conditional_expr_converter(["1","2","3"])
		Out[]: ["1", "2", "3"]
	
		In []: conditional_expr_converter({'allOf': ["3", "1", "2"]})
		Out[]: [["3", "1", "2"]]
	
		In []: conditional_expr_converter({'anyOf': ["3", "1", "2"]})
		Out[]: ["3", "1", "2"]
	
		In []: conditional_expr_converter({'anyOf': [{'allOf': ["1","2"]}, "3", "1", "2"]})
		Out[]: [["1", "2"], "3", "1", "2"]
	
		In []: conditional_expr_converter({'anyOf': [{'allOf': ["1","2"]}, "3", {'allOf': ["1","3"]}]})
		Out[]: [["1", "2"], "3", ["1", "3"]]
	
		In []: conditional_expr_converter(["1", "2", ["1", "2""3"]])
		ValueError: Unsupported format (0)
	
		In []: conditional_expr_converter({'allOf': ["1", "2", ["1","2"]]})
		ValueError: Unsupported nesting
	
		In []: conditional_expr_converter({'allOf': ["1", "2"], 'abc': "2"})
		ValueError: Unsupported format ("1")
	
		In []: conditional_expr_converter({'anyOf': [{'anyOf': ["1","2"]}, "2"]})
		ValueError: Unsupported nesting
	
		In []: conditional_expr_converter({'anyOf': [{'allOf': ["1","2"]}, "3", {'anyOf': ["1","2"]}]})
		ValueError: Unsupported nesting
	
		In []: conditional_expr_converter({'allOf': [{'allOf': ["1","2"]}, "3", "1", "2"]})
		ValueError: Unsupported nesting
	
		In []: conditional_expr_converter({'anyOf': [{'allOf': ["1","2"]}, "3", {'allOf': ["1",{'allOf':["1","2"]}]}]})
		ValueError: Unsupported nesting
		```
		"""
	
		sequences = (list, tuple)
		ok = (str, int, float)
	
		if level > 2:
			raise ValueError("Unsupported nesting level")
	
		if type(arg) in (str, int, float):
			return arg
	
		if type(arg) in sequences:
			if not AmpelUtils.check_seq_inner_type(arg, ok, multi_type=True):
				raise ValueError("Unsupported format (0)")
			return arg
		
		if type(arg) is dict:
	
			if len(arg) != 1:
				raise ValueError("Unsupported format (1)")
	
			key = next(iter(arg.keys()))
	
			if key == "allOf":
	
				# Value must be a sequence
				if type(arg[key]) not in sequences:
					raise ValueError("Unsupported format (3)")
	
				# 'allOf' closes nesting (content must be atomar elements of type 'ok') 
				if not AmpelUtils.check_seq_inner_type(arg[key], ok, multi_type=True):
					raise ValueError("Unsupported nesting")
	
				return [arg[key]] if level == 1 else arg[key]
	
			elif key == "anyOf":
	
				if level > 1:
					raise ValueError("Unsupported nesting")
	
				# Value must be a sequence
				if type(arg[key]) not in sequences:
					raise ValueError("Unsupported format (4)")
	
				if AmpelUtils.check_seq_inner_type(arg[key], ok, multi_type=True):
					return arg[key]
	
				return [cls.conditional_expr_converter(el, level=level+1) for el in arg[key]]
	
			else:
				raise ValueError("Unsupported format (5)")
	
		else:
			raise ValueError("Unsupported format (6)")
