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
				" -> {'anyOf': ['Ab', 'Cd']} or\n" +
				" -> {'allOf': ['Ab', 'Cd']} or\n" +
				" -> {'oneOf': ['Ab', 'Cd']} or\n" +
				"One level nesting is allowed, please see\n" +
				"ConfigUtils.conditional_expr_converter(..) docstring for more info"
			)

		if isinstance(v, (str, int)):
			return {'anyOf': [v]}

		if isinstance(v, dict):

			if len(v) != 1:
				raise ValueError(
					"transients->select->%s config error\n" % field_name +
					"Unsupported dict format %s" % v
				)

			if 'anyOf' in v:

				if not isinstance(v['anyOf'], strict_iterable):
					raise ValueError(
						"transients->select->%s:anyOf config error\n" % field_name +
						"Invalid dict value type: %s. Must be a sequence" % type(v['anyOf'])
					)

				# 'anyOf' supports only a list of dicts and str/int
				if not check_seq_inner_type(v['anyOf'], (str, int, dict), multi_type=True):
					raise ValueError(
						"transients->select->%s:anyOf config error\n" % field_name +
						"Unsupported nesting (err 2)"
					)

				if not check_seq_inner_type(v['anyOf'], (int, str)) and len(v['anyOf']) < 2:
					raise ValueError(
						"transients->select->%s:anyOf config error\n" % field_name +
						"anyOf list must contain more than one element when containing allOf\n" +
						"Offending value: %s" % v
					)

				for el in v['anyOf']:

					if isinstance(el, dict):

						if 'anyOf' in el:
							raise ValueError(
								"transients->select->%s:anyOf.anyOf config error\n" % field_name +
								"Unsupported nesting (anyOf in anyOf)"
							)

						if 'allOf' in el:

							# 'allOf' closes nesting
							if not check_seq_inner_type(el['allOf'], (int, str)):
								raise ValueError(
									"transients->select->%s:anyOf.allOf config error\n" % field_name +
									"Unsupported nesting (allOf list content must be str/int)"
								)

							if len(set(el['allOf'])) < 2:
								raise ValueError(
									"transients->select->%s:allOf config error\n" % field_name +
									"Please do not use allOf with just one element\n" +
									"Offending value: %s" % el
								)

						else:
							raise ValueError(
								"transients->select->%s:anyOf config error\n" % field_name +
								"Unsupported nested dict: %s" % el
							)

			elif 'allOf' in v:

				# 'allOf' closes nesting
				if (
					not isinstance(v['allOf'], strict_iterable) or
					not check_seq_inner_type(v['allOf'], (int, str))
				):
					raise ValueError(
						"transients->select->%s:allOf config error\n" % field_name +
						"Invalid type for value %s\n(must be a sequence, is: %s)\n" %
						(v['allOf'], type(v['allOf'])) +
						"Note: no nesting is allowed below 'allOf'"
					)

				if len(set(v['allOf'])) < 2:
					raise ValueError(
						"transients->select->%s:allOf config error\n" % field_name +
						"Please do not use allOf with just one element\n" +
						"Offending value: %s" % v
					)

			elif 'oneOf' in v:

				# 'oneOf' closes nesting
				if (
					not isinstance(v['oneOf'], strict_iterable) or
					not check_seq_inner_type(v['oneOf'], (int, str))
				):
					raise ValueError(
						"transients->select->%s:oneOf config error\n" % field_name +
						"Invalid type for value %s\n(must be a sequence, is: %s)\n" %
						(v['oneOf'], type(v['oneOf'])) +
						"Note: no nesting is allowed below 'oneOf'"
					)

			else:
				raise ValueError(
					"transients->select->%s config error\n" % field_name +
					"Invalid dict key (only 'anyOf', 'allOf', 'oneOf' are allowed)"
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
			Schema: {'anyOf': ['a', 'b', 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'allOf': ['a', 'b', 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'anyOf': [{'allOf': ['a', 'b']}, 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'anyOf': [{'allOf': ['a', 'b']}, {'allOf': ['a', 'c']}, 'd']}
			Reduced set: {'d', 'b', 'a', 'c'}
		"""

		if isinstance(arg, in_type):
			return {arg}

		if isinstance(arg, (AllOf, AnyOf, OneOf)):
			arg = arg.dict()

		if isinstance(arg, dict):

			if "anyOf" in arg:
				s = set()
				for el in arg['anyOf']:
					if isinstance(el, in_type):
						s.add(el)
					elif isinstance(el, dict):
						for ell in next(iter(el.values())):
							s.add(ell)
					else:
						raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (1)")
				return s

			if 'allOf' in arg:
				return set(arg['allOf'])

			if 'oneOf' in arg:
				return set(arg['oneOf'])

			raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (2)")

		raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported type: %s" % type(arg))


	@classmethod
	def conditional_expr_converter(cls, arg, level=1):
		"""
		Converts JSON encoded conditional statements from Ampel config file
		into arrays with dimension up to two.
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

			if key == "allOf":

				# Value must be a sequence
				if not isinstance(arg[key], sequences):
					raise ValueError("Unsupported format (3)")

				# 'allOf' closes nesting (content must be atomar elements of type 'ok')
				if not check_seq_inner_type(arg[key], ok, multi_type=True):
					raise ValueError("Unsupported nesting")

				return [arg[key]] if level == 1 else arg[key]

			if key == "anyOf":

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
