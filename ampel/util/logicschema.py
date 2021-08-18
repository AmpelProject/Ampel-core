#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-interface/ampel/util/logicschema.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.04.2021
# Last Modified Date: 03.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Dict, Tuple, Type, Sequence, Literal, Any, Set
from ampel.types import T, strict_iterable
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.util.collections import check_seq_inner_type


def to_logical_dict(v, field_name: Union[int, str, Dict[str, Any]]) -> Dict[str, Any]:
	"""
	Converts str/int into {'any_of': int/str}.
	Checks structure in case dict is provided
	(how deep potential nested logic are, raises error in case of incorrect logic).
	:raises ValueError: in case provided argument is incorrect
	"""

	if isinstance(v, list):
		raise ValueError(
			"stock->select->%s config error\n" % field_name +
			"'%s' parameter cannot be a list. " % field_name +
			"Please use the following syntax:\n" +
			" -> {'any_of': ['Ab', 'Cd']} or\n" +
			" -> {'all_of': ['Ab', 'Cd']} or\n" +
			" -> {'one_of': ['Ab', 'Cd']} or\n" +
			"One level nesting is allowed, please see\n" +
			"conditional_expr_converter(..) docstring for more info"
		)

	if isinstance(v, (str, int)):
		return {'any_of': [v]}

	if isinstance(v, dict):

		if len(v) != 1:
			raise ValueError(
				"stock->select->%s config error\n" % field_name +
				"Unsupported dict format %s" % v
			)

		if 'any_of' in v:

			if not isinstance(v['any_of'], strict_iterable):
				raise ValueError(
					"stock->select->%s:any_of config error\n" % field_name +
					"Invalid dict value type: %s. Must be a sequence" % type(v['any_of'])
				)

			# 'any_of' supports only a list of dicts and str/int
			if not check_seq_inner_type(v['any_of'], (str, int, dict), multi_type=True):
				raise ValueError(
					"stock->select->%s:any_of config error\n" % field_name +
					"Unsupported nesting (err 2)"
				)

			if not check_seq_inner_type(v['any_of'], (int, str)) and len(v['any_of']) < 2:
				raise ValueError(
					"stock->select->%s:any_of config error\n" % field_name +
					"any_of list must contain more than one element when containing all_of\n" +
					"Offending value: %s" % v
				)

			for el in v['any_of']:

				if isinstance(el, dict):

					if 'any_of' in el:
						raise ValueError(
							"stock->select->%s:any_of.any_of config error\n" % field_name +
							"Unsupported nesting (any_of in any_of)"
						)

					if 'all_of' in el:

						# 'all_of' closes nesting
						if not check_seq_inner_type(el['all_of'], (int, str)):
							raise ValueError(
								"stock->select->%s:any_of.all_of config error\n" % field_name +
								"Unsupported nesting (all_of list content must be str/int)"
							)

						if len(set(el['all_of'])) < 2:
							raise ValueError(
								"stock->select->%s:all_of config error\n" % field_name +
								"Please do not use all_of with just one element\n" +
								"Offending value: %s" % el
							)

					else:
						raise ValueError(
							"stock->select->%s:any_of config error\n" % field_name +
							"Unsupported nested dict: %s" % el
						)

		elif 'all_of' in v:

			# 'all_of' closes nesting
			if (
				not isinstance(v['all_of'], strict_iterable) or
				not check_seq_inner_type(v['all_of'], (int, str))
			):
				raise ValueError(
					"stock->select->%s:all_of config error\n" % field_name +
					"Invalid type for value %s\n(must be a sequence, is: %s)\n" %
					(v['all_of'], type(v['all_of'])) +
					"Note: no nesting is allowed below 'all_of'"
				)

			if len(set(v['all_of'])) < 2:
				raise ValueError(
					"stock->select->%s:all_of config error\n" % field_name +
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
					"stock->select->%s:one_of config error\n" % field_name +
					"Invalid type for value %s\n(must be a sequence, is: %s)\n" %
					(v['one_of'], type(v['one_of'])) +
					"Note: no nesting is allowed below 'one_of'"
				)

		else:
			raise ValueError(
				"stock->select->%s config error\n" % field_name +
				"Invalid dict key (only 'any_of', 'all_of', 'one_of' are allowed)"
			)

	return v


def reduce_to_set(
	arg: Union[
		T,
		# unsure if mypy understands unions of dicts with different key literals actually
		Dict[Union[Literal['all_of'], Literal['one_of']], Sequence[T]],
		Dict[Literal['any_of'], Union[Sequence[T], Dict[Literal['all_of'], Sequence[T]]]],
		AllOf[T], AnyOf[T], OneOf[T]
	],
	in_type: Tuple[Type, ...] = (str, int)
) -> Set[T]:
	"""
	.. sourcecode:: python\n
		for schema in (a,b,c,d,e):
			print("Schema: %s" % schema)
			print("Reduced set: %s" % reduce_to_set(schema))

		Schema: 'a'
		Reduced set: {'a'}
		Schema: {'any_of': ['a', 'b', 'c']}
		Reduced set: {'b', 'a', 'c'}
		Schema: {'all_of': [1, 2, 3]}
		Reduced set: {1, 2, 3}
		Schema: {'any_of': [{'all_of': ['a', 'b']}, 'c']}
		Reduced set: {'b', 'a', 'c'}
		Schema: {'any_of': [{'all_of': ['a', 'b']}, {'all_of': ['a', 'c']}, 'd']}
		Reduced set: {'d', 'b', 'a', 'c'}
	"""

	if isinstance(arg, in_type):
		return {arg} # type: ignore

	elif isinstance(arg, (AllOf, AnyOf, OneOf)):
		v: Any = arg.dict()

	elif isinstance(arg, dict):
		v = arg

	else:
		raise ValueError("Unsupported arg type: %s" % type(arg))

	if "any_of" in v:
		s = set()
		for el in v['any_of']:
			if isinstance(el, in_type):
				s.add(el)
			elif isinstance(el, dict):
				for ell in next(iter(el.values())):
					s.add(ell)
			else:
				raise ValueError("unsupported format (1)")
		return s

	if 'all_of' in v:
		return set(v['all_of'])

	if 'one_of' in v:
		return set(v['one_of'])

	raise ValueError("unsupported format (2)")
