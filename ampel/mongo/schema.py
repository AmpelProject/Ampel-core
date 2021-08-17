#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/schema.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.03.2018
# Last Modified Date: 26.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Tuple, Dict, List, Type
from ampel.util.collections import check_seq_inner_type
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf

VALID_TYPES = (int, str)

"""
Schema syntax example:

.. sourcecode:: python\n
	{'any_of': ["a", "b", "c"]}
	{'all_of': ["a", "b", "c"]}
	{'one_of': ["a", "b", "c"]}
	{'any_of': [{'all_of': ["a","b"]}, "c"]}
	{'any_of': [{'all_of': ["a","b"]}, {'all_of': ["a","c"]}, "d"]}


	In [1]: from ampel.mongo.schema import apply_schema, apply_excl_schema
	   ...: from ampel.util.pretty import prettyjson
	   ...: d={'$or': [{'run': 12}, {'run': 231}]}
	   ...: _=apply_schema(d, 'tag', {'any_of': [{'all_of': ["a","b"]}, "3", "1", "2"]})
	   ...: _=apply_excl_schema(d, 'channel', {'any_of': [{'all_of': ["CHAN_A","CHAN_B"]}, "CHAN_Z"]})
	   ...: print(prettyjson(d))
	{
	  "$and": [
		{"$or": [{"run": 12}, {"run": 231}]},
		{"$or": [{"tag": {"$all": ["a", "b"]}}, {"tag": {"$in": ["3", "1", "2"]}}]},
		{"channel": {"$not": {"$all": ["CHAN_A", "CHAN_B"]}}},
		{"channel": {"$ne": "CHAN_Z"}}
	  ]
	}

	In []: d={'$or': [{'run': 12}, {'run': 231}]}
	   ...: _=apply_excl_schema(d, 'channel', {'any_of': [{'all_of': ["CHAN_A","CHAN_B"]}, "CHAN_Z"]})
	   ...: _=apply_schema(d, 'tag', {'any_of': [{'all_of': ["a","b"]}, "3", "1", "2"]})
	   ...: print(prettyjson(d))
	{
	  "$and": [
		{"channel": {"$not": {"$all": ["CHAN_A", "CHAN_B"]}}},
		{"channel": {"$ne": "CHAN_Z"}},
		{"$or": [{"run": 12}, {"run": 231}]},
		{"$or": [{"tag": {"$all": ["a", "b"]}}, {"tag": {"$in": ["3", "1", "2"]}}]}
	  ]
	}

"""

def apply_schema(
	query: dict, field_name: str,
	arg: Union[int, str, dict, AllOf, AnyOf, OneOf]
) -> Dict:
	"""
	Warning: The method changes keys and values in the input dict parameter "query"

	This function translates a dict schema containing conditional (AND/OR) matching criteria
	into a dict syntax that mongodb understands. It is typically used for queries
	whose selection criteria are loaded directly from config documents.

	:param query: dict that will be later used as query criteria (can be empty).
	This dict will be updated with the additional matching criteria computed by this method.

	:param field_name: name of the target DB field

	:param arg: for convenience, the parameter can be of type int/str if only one value is to be matched.
	The dict can be nested up to one level.
	Please see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` docstring for details.

	:raises ValueError: if 'query' already contains the value of 'field_name' as key

	:returns: (modified/updated) dict instance referenced by query
	"""

	# Validations already performed by models
	if isinstance(arg, (AllOf, AnyOf, OneOf)):
		arg_dict = arg.dict()
	elif isinstance(arg, (int, str)):
		query[field_name] = arg
		return query
	else:
		arg_dict = arg

	if 'all_of' in arg_dict:

		# Raises error if invalid
		_check_all_of(arg_dict, VALID_TYPES)

		if len(arg_dict['all_of']) == 1:
			query[field_name] = arg_dict['all_of'][0]
		else:
			query[field_name] = {'$all': arg_dict['all_of']}

	elif 'any_of' in arg_dict:

		# Case 1: no nesting below any_of
		if check_seq_inner_type(arg_dict['any_of'], VALID_TYPES):
			if len(arg_dict['any_of']) == 1: # dumb case
				query[field_name] = arg_dict['any_of'][0]
			else:
				query[field_name] = {'$in': arg_dict['any_of']}

		# Case 2: nesting below any_of
		else:

			QueryValue = Union[int, str, Dict[str, List[Union[int, str]]]]
			QueryList = List[Dict[str, QueryValue]]
			or_list: QueryList = []
			optimize_potentially: QueryList = []

			for el in arg_dict['any_of']:

				if isinstance(el, VALID_TYPES):
					or_list.append(
						{field_name: el}
					)
					optimize_potentially.append(or_list[-1])

				elif isinstance(el, dict):

					# Raises error if invalid
					_check_all_of(el, VALID_TYPES)

					if len(el['all_of']) == 1:
						or_list.append(
							{field_name: el['all_of'][0]}
						)
						optimize_potentially.append(or_list[-1])
					else:
						or_list.append(
							{field_name: {'$all': el['all_of']}}
						)

				else:
					raise ValueError(
						f"Unsupported format: depth 2 element is not a of type {VALID_TYPES} or dict)"
						f"\nOffending value: {el}"
					)


			# ex: apply_schema(query, 'tag', {'any_of': [{'all_of': ["a","b"]}, "3", "1", "2"]})
			# optimize_potentially enables to replace:
			#   '$or': [
			#		{'tag': {'$all': ['a', 'b']}},
			#   	{'tag': '3'}, {'tag': '1'}, {'tag': '2'}
			#	]
			# with:
			#   '$or': [
			#		{'tag': {'$all': ['a', 'b']}},
			#   	{'tag': {'$in': ['3', '1', '2']}}
			#	]
			if len(optimize_potentially) > 1:
				or_list.append(
					{
						field_name: {
							'$in': [
								item
								for el in optimize_potentially
								if isinstance((item := el[field_name]), (int, str))
							]
						}
					}
				)
				for el in optimize_potentially:
					or_list.remove(el)

			if len(or_list) == 1:
				query[field_name] = or_list[0][field_name]
			else:
				if '$or' in query:
					prev_or = {'$or': query.pop('$or')}
					if '$and' in query:
						query['$and'].append(prev_or)
					else:
						query['$and'] = [prev_or]
					query['$and'].append({'$or': or_list})
				else:
					query['$or'] = or_list

	elif 'one_of' in arg_dict:
		query[field_name] = arg_dict['one_of']

	else:
		raise ValueError(
			"Invalid 'arg_dict' keys (must contain either 'any_of' or 'all_of'" +
			"\nOffending value: %s" % arg_dict
		)

	return query


def apply_excl_schema(
	query: dict, field_name: str,
	arg: Union[int, str, dict, AllOf, AnyOf, OneOf]
) -> Dict:
	"""
	Warning: The method changes keys and values in the input dict parameter "query"
	Parameters: see docstring of apply_schema
	:returns: dict instance referenced by parameter 'query' (which was updated)
	"""

	# Checks were already performed by validators
	if isinstance(arg, (AllOf, AnyOf, OneOf)):
		arg = arg.dict()
	else:
		_arg_check(arg)

	# Check if field_name criteria were set previously
	if field_name in query:
		if isinstance(query[field_name], VALID_TYPES):
			# If a previous scalar tag matching criteria was set
			# then we need rename it since query[field_name] will become a dict
			query[field_name] = {'$eq': query[field_name]}

	if isinstance(arg, VALID_TYPES):
		if field_name not in query:
			query[field_name] = {}
		query[field_name]['$ne'] = arg
		return query

	if not isinstance(arg, dict):
		raise ValueError('Illegal "arg" parameter')

	if 'all_of' in arg:

		# Raises error if invalid
		_check_all_of(arg, VALID_TYPES)

		if field_name not in query:
			query[field_name] = {}

		if len(arg['all_of']) == 1: # dumb case
			query[field_name]['$ne'] = arg['all_of'][0]
		else:
			query[field_name]['$not'] = {'$all': arg['all_of']}

	elif 'any_of' in arg:

		# Case 1: no nesting below any_of
		if check_seq_inner_type(arg['any_of'], VALID_TYPES):

			if field_name not in query:
				query[field_name] = {}

			if len(arg['any_of']) == 1: # dumb case
				query[field_name]['$ne'] = arg['any_of'][0]
			else:
				query[field_name]['$not'] = {'$all': arg['any_of']}

		else:

			QueryValue = Union[int, str, Dict[str, Union[int, str]]]
			QueryList = List[Dict[str, QueryValue]]
			and_list: QueryList = []
			optimize_potentially: QueryList = []

			for el in arg['any_of']:

				if isinstance(el, VALID_TYPES):
					and_list.append(
						{field_name: {'$ne': el}}
					)
					optimize_potentially.append(and_list[-1])

				elif isinstance(el, dict):

					# Raises error if el is invalid
					_check_all_of(el, VALID_TYPES)

					if len(el['all_of']) == 1:
						and_list.append(
							{field_name: {'$ne': el['all_of'][0]}}
						)
						optimize_potentially.append(and_list[-1])
					else:
						and_list.append(
							{
								field_name: {
									'$not': {'$all': el['all_of']} # type: ignore[dict-item]
								}
							}
						)
				else:
					raise ValueError(
						f"Unsupported format: depth 2 element is not a {VALID_TYPES} or dict)"
						f"\nOffending value: {el}"
					)

			# ex: add_from_excl_dict(query, 'tag', {'any_of': [{'all_of': ["a","b"]}, "3", "1", "2"]})
			# optimize_potentially allows to replace:
			#   '$and': [
			#		{'tag': {'$not': {'$all': ['a', 'b']}}},
			#   	{'tag': {'$ne': '3'}}, {'tag': {'$ne': '1'}}, {'tag': {'$ne': '2'}}
			#	]
			# with:
			#   '$and': [
			#		{'tag': {'$not': {'$all': ['a', 'b']}}},
			#   	{'tag': {'$nin': ['3', '1', '2']}}
			#	]
			if len(optimize_potentially) > 1:
				and_list.append(
					{
						field_name: {
							'$nin': [el[field_name]['$ne'] for el in optimize_potentially] # type: ignore
						}
					}
				)
				for el in optimize_potentially:
					and_list.remove(el)

			if len(and_list) == 1:
				query[field_name] = and_list[0][field_name]
			else:
				if '$and' in query:
					query['$and'].extend(and_list)
				else:
					query['$and'] = and_list

	elif 'one_of' in arg:
		query[field_name] = arg['one_of']

	else:
		raise ValueError(
			f"Invalid 'arg' keys (must contain either 'any_of' or 'all_of'"
			f"\nOffending value: {arg}"
		)

	return query


def _check_all_of(el, in_type: Tuple[Type, ...]) -> None:
	""" :raises: ValueError """

	if 'all_of' not in el:
		raise ValueError(
			f"Expected dict with key 'all_of'"
			f"\nOffending value: {el}"
		)

	if not check_seq_inner_type(el['all_of'], in_type):
		raise ValueError(
			f"No further nesting allowed beyond 'all_of'"
			f"\nOffending value: {el}"
		)

	if len(el) != 1:
		raise ValueError(
			f"'all_of' dict should have only one key"
			f"\nOffending value: {el}"
		)


def _arg_check(arg) -> bool:
	""" :raises: ValueError """

	if arg is None:
		raise ValueError('"arg" is None')

	if isinstance(arg, (int, str, tuple)):
		return True

	# Dict must be provided
	if not isinstance(arg, dict):
		raise ValueError(
			f"Parameter 'arg' should be a dict (is {type(arg)})"
		)

	# With only one key
	if len(arg) != 1:
		raise ValueError(
			f"Invalid 'arg' parameter. Should have only one key ('any_of' or 'all_of')"
			f"\nOffending value: {arg}"
		)

	return False
