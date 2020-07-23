#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/LogicSchemaUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.10.2018
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.type import strict_iterable
from ampel.util.collections import check_seq_inner_type
from ampel.config.LogicSchemaIterator import LogicSchemaIterator
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf

class LogicSchemaUtils:


	@staticmethod
	def iter(arg):
		"""
		Please see :obj:`LogicSchemaIterator <ampel.config.t3.LogicSchemaIterator>` \
		docstring for more info
		"""
		return LogicSchemaIterator(arg)


	@staticmethod
	def to_logical_struct(v, field_name):

		if isinstance(v, list):
			raise ValueError(
				"transients->select->%s config error\n" % field_name +
				"'%s' parameter cannot be a list. " % field_name +
				"Please use the following syntax:\n" +
				" -> {'any_of': ['Ab', 'Cd']} or\n" +
				" -> {'all_of': ['Ab', 'Cd']} or\n" +
				" -> {'one_of': ['Ab', 'Cd']} or\n" +
				"One level nesting is allowed, please see\n" +
				"ConfigUtils.conditional_expr_converter(..) docstring for more info"
			)

		if isinstance(v, (str, int)):
			return {'any_of': [v]}

		if isinstance(v, dict):

			if len(v) != 1:
				raise ValueError(
					"transients->select->%s config error\n" % field_name +
					"Unsupported dict format %s" % v
				)

			if 'any_of' in v:

				if not isinstance(v['any_of'], strict_iterable):
					raise ValueError(
						"transients->select->%s:any_of config error\n" % field_name +
						"Invalid dict value type: %s. Must be a sequence" % type(v['any_of'])
					)

				# 'any_of' supports only a list of dicts and str/int
				if not check_seq_inner_type(v['any_of'], (str, int, dict), multi_type=True):
					raise ValueError(
						"transients->select->%s:any_of config error\n" % field_name +
						"Unsupported nesting (err 2)"
					)

				if not check_seq_inner_type(v['any_of'], (int, str)) and len(v['any_of']) < 2:
					raise ValueError(
						"transients->select->%s:any_of config error\n" % field_name +
						"any_of list must contain more than one element when containing all_of\n" +
						"Offending value: %s" % v
					)

				for el in v['any_of']:

					if isinstance(el, dict):

						if 'any_of' in el:
							raise ValueError(
								"transients->select->%s:any_of.any_of config error\n" % field_name +
								"Unsupported nesting (any_of in any_of)"
							)

						if 'all_of' in el:

							# 'all_of' closes nesting
							if not check_seq_inner_type(el['all_of'], (int, str)):
								raise ValueError(
									"transients->select->%s:any_of.all_of config error\n" % field_name +
									"Unsupported nesting (all_of list content must be str/int)"
								)

							if len(set(el['all_of'])) < 2:
								raise ValueError(
									"transients->select->%s:all_of config error\n" % field_name +
									"Please do not use all_of with just one element\n" +
									"Offending value: %s" % el
								)

						else:
							raise ValueError(
								"transients->select->%s:any_of config error\n" % field_name +
								"Unsupported nested dict: %s" % el
							)

			elif 'all_of' in v:

				# 'all_of' closes nesting
				if (
					not isinstance(v['all_of'], strict_iterable) or
					not check_seq_inner_type(v['all_of'], (int, str))
				):
					raise ValueError(
						"transients->select->%s:all_of config error\n" % field_name +
						"Invalid type for value %s\n(must be a sequence, is: %s)\n" %
						(v['all_of'], type(v['all_of'])) +
						"Note: no nesting is allowed below 'all_of'"
					)

				if len(set(v['all_of'])) < 2:
					raise ValueError(
						"transients->select->%s:all_of config error\n" % field_name +
						"Please do not use all_of with just one element\n" +
						"Offending value: %s" % v
					)

			elif 'one_of' in v:

				# 'one_of' closes nesting
				if (
					not isinstance(v['one_of'], strict_iterable) or
					not check_seq_inner_type(v['one_of'], (int, str))
				):
					raise ValueError(
						"transients->select->%s:one_of config error\n" % field_name +
						"Invalid type for value %s\n(must be a sequence, is: %s)\n" %
						(v['one_of'], type(v['one_of'])) +
						"Note: no nesting is allowed below 'one_of'"
					)

			else:
				raise ValueError(
					"transients->select->%s config error\n" % field_name +
					"Invalid dict key (only 'any_of', 'all_of', 'one_of' are allowed)"
				)

		return v


	@staticmethod
	def reduce_to_set(arg, in_type=(str, int)):
		"""
		.. sourcecode:: python\n
			for schema in (a,b,c,d,e):
				print("Schema: %s" % schema)
				print("Reduced set: %s" % LogicSchemaUtils.reduce_to_set(schema))

			Schema: 'a'
			Reduced set: {'a'}
			Schema: {'any_of': ['a', 'b', 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'all_of': ['a', 'b', 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'any_of': [{'all_of': ['a', 'b']}, 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'any_of': [{'all_of': ['a', 'b']}, {'all_of': ['a', 'c']}, 'd']}
			Reduced set: {'d', 'b', 'a', 'c'}
		"""

		if isinstance(arg, in_type):
			return {arg}

		if isinstance(arg, (AllOf, AnyOf, OneOf)):
			arg = arg.dict()

		if isinstance(arg, dict):

			if "any_of" in arg:
				s = set()
				for el in arg['any_of']:
					if isinstance(el, in_type):
						s.add(el)
					elif isinstance(el, dict):
						for ell in next(iter(el.values())):
							s.add(ell)
					else:
						raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (1)")
				return s

			if 'all_of' in arg:
				return set(arg['all_of'])

			if 'one_of' in arg:
				return set(arg['one_of'])

			raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (2)")

		raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported type: %s" % type(arg))


	@classmethod
	def conditional_expr_converter(cls, arg, level=1):
		"""
		Converts JSON encoded conditional statements from Ampel config file
		into arrays with dimension up to two.
		'any_of' -> or operator -> encoded in a array elements of depth=1
		'all_of' -> and operator -> encoded in array elements of depth=2

		Accepted input:
		---------------

		atomar values str, int float: "a" / 1 / 1.2

		1d sequences of atomar values (automaticall_ofy treated as 'any_of' sequence):
		[1, 2, 3]  / [1, "a", 3.4]

		'any_of' dict containing 1d list of atomar values (explicit 'any_of' sequence):
		{'any_of': [1, 2, 3]} / {'any_of': [1, "a", 3.4]}

		'all_of' dict containing 1d list of atomar values
		{'all_of': [1, 2, 3]} / {'any_of': [1, "a", 3.4]}

		Nested structure whereby 'all_of' closes the nesting (can contain only a sequence of atomar values)
		{
			'any_of': [
				{'all_of': ["HUSN1", "HUSN2"]},
				"HUBH1",
				{'all_of': ["HUSN1", "HUSN3"]}
			]
		}

		Examples:
		---------

		```
		In []: conditional_expr_converter("abc")
		Out[]: 'abc'

		In []: conditional_expr_converter(["1","2","3"])
		Out[]: ["1", "2", "3"]

		In []: conditional_expr_converter({'all_of': ["3", "1", "2"]})
		Out[]: [["3", "1", "2"]]

		In []: conditional_expr_converter({'any_of': ["3", "1", "2"]})
		Out[]: ["3", "1", "2"]

		In []: conditional_expr_converter({'any_of': [{'all_of': ["1","2"]}, "3", "1", "2"]})
		Out[]: [["1", "2"], "3", "1", "2"]

		In []: conditional_expr_converter({'any_of': [{'all_of': ["1","2"]}, "3", {'all_of': ["1","3"]}]})
		Out[]: [["1", "2"], "3", ["1", "3"]]

		In []: conditional_expr_converter(["1", "2", ["1", "2""3"]])
		ValueError: Unsupported format (0)

		In []: conditional_expr_converter({'all_of': ["1", "2", ["1","2"]]})
		ValueError: Unsupported nesting

		In []: conditional_expr_converter({'all_of': ["1", "2"], 'abc': "2"})
		ValueError: Unsupported format ("1")

		In []: conditional_expr_converter({'any_of': [{'any_of': ["1","2"]}, "2"]})
		ValueError: Unsupported nesting

		In []: conditional_expr_converter({'any_of': [{'all_of': ["1","2"]}, "3", {'any_of': ["1","2"]}]})
		ValueError: Unsupported nesting

		In []: conditional_expr_converter({'all_of': [{'all_of': ["1","2"]}, "3", "1", "2"]})
		ValueError: Unsupported nesting

		In []: conditional_expr_converter({'any_of': [{'all_of': ["1","2"]}, "3", {'all_of': ["1",{'all_of':["1","2"]}]}]})
		ValueError: Unsupported nesting
		```
		"""

		sequences = (list, tuple)
		ok = (str, int, float)

		if level > 2:
			raise ValueError("Unsupported nesting level")

		if isinstance(arg, (str, int, float)):
			return arg

		if isinstance(arg, sequences):
			if not check_seq_inner_type(arg, ok, multi_type=True):
				raise ValueError("Unsupported format (0)")
			return arg

		if isinstance(arg, dict):

			if len(arg) != 1:
				raise ValueError("Unsupported format (1)")

			key = next(iter(arg.keys()))

			if key == "all_of":

				# Value must be a sequence
				if not isinstance(arg[key], sequences):
					raise ValueError("Unsupported format (3)")

				# 'all_of' closes nesting (content must be atomar elements of type 'ok')
				if not check_seq_inner_type(arg[key], ok, multi_type=True):
					raise ValueError("Unsupported nesting")

				return [arg[key]] if level == 1 else arg[key]

			if key == "any_of":

				if level > 1:
					raise ValueError("Unsupported nesting")

				# Value must be a sequence
				if not isinstance(arg[key], sequences):
					raise ValueError("Unsupported format (4)")

				if check_seq_inner_type(arg[key], ok, multi_type=True):
					return arg[key]

				return [cls.conditional_expr_converter(el, level=level + 1) for el in arg[key]]

			raise ValueError("Unsupported format (5)")

		raise ValueError("Unsupported format (6)")
