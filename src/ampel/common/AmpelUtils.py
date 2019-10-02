#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/common/AmpelUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 02.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections, hashlib
from functools import reduce
from ampel.config.ReadOnlyDict import ReadOnlyDict

class AmpelUtils():
	""" 
	Static methods (as of Sept 2019):
	iter(arg)
	try_reduce(arg)
	is_sequence(obj)
	to_set(arg)
	check_seq_inner_type(seq, types, multi_type=False)
	get_by_path(mapping, path, delimiter='.')
	flatten_dict(d, separator='.')
	unflatten_dict(d, separator='.')
	"""

	@staticmethod
	def iter(arg):
		"""
		-> suppressing python3 treatment of str as iterable (a really dumb choice...)
		-> Making None iterable
		"""
		return [arg] if isinstance(arg, (type(None), str, int, bytes, bytearray)) else arg


	@staticmethod
	def try_reduce(arg):
		"""
		Returns element contained by sequence if sequence contains only one element.
		Example:
		try_reduce(['ab']) -> returns 'ab'
		try_reduce({'ab'}) -> returns 'ab'
		try_reduce(['a', 'b']) -> returns ['a', 'b']
		try_reduce({'a', 'b'}) -> returns {'a', 'b'}
		"""
		if len(arg) == 1:
			return next(iter(arg))

		return arg


	@staticmethod
	def is_sequence(arg):
		"""
		False if str, bytes, bytearrsay
		True is instance of collections.abc.Sequence
		"""
		if arg is None:
			return None

		return isinstance(arg, collections.abc.Sequence) and not isinstance(arg, (str, bytes, bytearray))


	@staticmethod
	def build_unsafe_dict_id(dict_arg):
		"""
		:param dict dict_arg: can be nested
		:returns: short dict id (ex: 'b2202f') made of a the last 6 characters of a SHA1 hex string.
		:rtype: str
		"""
		import json, hashlib
		return hashlib.sha1(
			bytes(
				json.dumps(
					dict_arg, sort_keys=True, 
					indent=None, separators=(',', ':')
				),
				"utf8"
			)
		).hexdigest()[-6:]


	@staticmethod
	def to_set(arg):
		"""
		1) Reminder of python deep logic:
		In []: set('abc')
		Out[]: {'a', 'b', 'c'}
		In []: {'abc'}
		Out[]: {'abc'}

		2) Reminder of python unbreakable robustness:
		In []: set([1,2])
		Out[]: {1, 2}
		In []: {[1,2]}
		Out[]: -> TypeError: unhashable type: 'list'

		In []: AmpelUtils.to_set("abc")
		Out[]: {'abc'}
		In []: AmpelUtils.to_set(["abc"])
		Out[]: {'abc'}
		In []: AmpelUtils.to_set(['a','b','c'])
		Out[]: {'a', 'b', 'c'}
		In []: AmpelUtils.to_set([1,2])
		Out[]: {1, 2}
		"""
		return set(arg) if AmpelUtils.is_sequence(arg) else {arg}


	@classmethod
	def to_list(cls, arg):
		"""
		"""
		if isinstance(arg, (int, str, bytes, bytearray)):
			return [arg]
		if isinstance(arg, list):
			return arg
		if isinstance(arg, collections.abc.Iterable):
			return list(arg)

		raise ValueError("Unsupported format (%s)" % type(arg))


	@staticmethod
	def check_seq_inner_type(seq, types, multi_type=False):
		"""
		check type of all elements contained in a sequence.
		*all* members of the provided sequence must match with:
			* multi_type == False: one of the provided type.
			* multi_type == False: any of the provided type.

		check_seq_inner_type((1,2), int) -> True
		check_seq_inner_type([1,2], int) -> True
		check_seq_inner_type((1,2), float) -> False
		check_seq_inner_type(('a','b'), str) -> True
		check_seq_inner_type((1,2), (int, str)) -> True
		check_seq_inner_type((1,2,'a'), (int, str)) -> False
		check_seq_inner_type((1,2,'a'), (int, str), multi_type=True) -> True
		"""

		# monotype
		if not isinstance(types, collections.Sequence):
			return all(type(el) is types for el in seq)
		# different types accepted ('or' connected)
		else:
			if multi_type:
				return all(type(el) in types for el in seq)
			else:
				return any(
					tuple(AmpelUtils.check_seq_inner_type(seq, _type) for _type in types)
				)
	

	@staticmethod
	def get_by_path(mapping, path, delimiter='.'):
		"""
		Get an item from a nested mapping by path, e.g.
		'foo.bar.baz' -> mapping['foo']['bar']['baz']

		:param dict mapping: 
		:param path: example: 'foo.bar.baz' or ['foo', 'bar', 'baz']
		:type path: str, list
		:param str delimiter: example: '.'
		:rtype: object or None
		"""
		try:
			array = path.split(delimiter) if type(path) is str else path
			return reduce(lambda d, k: d.get(k), array, mapping)
		except AttributeError:
			return None


	@staticmethod
	def get_nested_attr(obj, path):
		"""
		Get a nested attribute from object:

		:param Object obj: 
		:param str path: example: 'foo.bar.baz'
		:rtype: object or None

		.. sourcecode:: python\n
			In []: time_constraint_config.before.value
			Out[]: 1531306299

			In []: AmpelUtils.get_nested_attr(time_constraint_config, "before.value")
			Out[]: 1531306299
		"""
		try:
			for name in path.split("."):
				obj = getattr(obj, name)
			return obj
		except AttributeError:
			return None


	@staticmethod
	def flatten_dict(d, separator='.'):
		"""
		Example: 
		input: {'count': {'chans': {'HU_SN': 10}}}
		output: {'count.chans.HU_SN': 10}
		"""
		expand = lambda key, val: (
			[(key + separator + k, v) for k, v in AmpelUtils.flatten_dict(val).items()] 
			if isinstance(val, dict) else [(key, val)]
		)

		items = [item for k, v in d.items() for item in expand(k, v)]

		return dict(items)


	@staticmethod
	def unflatten_dict(d, separator='.'):
		"""
		Example: 
		input: {'count.chans.HU_SN': 10}
		output: {'count': {'chans': {'HU_SN': 10}}}
		"""
		res = {}

		for key, value in d.items():

			parts = key.split(separator)
			d = res

			for part in parts[:-1]:
				if part not in d:
					d[part] = {}
				d = d[part]

			d[parts[-1]] = value

		return res
