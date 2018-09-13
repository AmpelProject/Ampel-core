#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 02.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections, logging, sys, traceback
from functools import reduce
from types import MappingProxyType
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class AmpelUtils():
	""" 
	Static methods (as of Sept 2019):
	iter(arg):
	try_reduce(arg):
	is_sequence(obj):
	to_set(arg):
	check_seq_inner_type(seq, types, multi_type=False):
	get_by_path(mapping, path, delimiter='.'):
	recursive_freeze(arg):
	flatten_dict(d, separator='.'):
	unflatten_dict(d, separator='.'):
	report_exception(logger, tier=None, info=None):
	"""

	@staticmethod
	def iter(arg):
		"""
		-> suppressing python3 treatment of str as iterable (a really dumb choice...)
		-> Making None iterable
		"""
		return [arg] if type(arg) in (type(None), str, bytes, bytearray) else arg


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
	def is_sequence(obj):
		"""
		False if str, bytes, bytearrsay
		True is instance of collections.abc.Sequence
		"""
		if obj is None:
			return None

		return isinstance(obj, collections.abc.Sequence) and not isinstance(obj, (str, bytes, bytearray))


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
		"""
		try:
			return reduce(lambda d, k: d.get(k), path.split(delimiter), mapping)
		except AttributeError:
			return None


	@staticmethod
	def recursive_freeze(arg):
		"""
		Return an immutable shallow copy
		:param arg:
			dict: MappingProxyType is returned
			list: tuple is returned
			set: frozenset is returned
			otherwise: arg is returned 'as is'
		"""
		if isinstance(arg, dict):
			return MappingProxyType(
				{
					AmpelUtils.recursive_freeze(k): AmpelUtils.recursive_freeze(v) 
					for k,v in arg.items()
				}
			)

		elif isinstance(arg, list):
			return tuple(
				map(AmpelUtils.recursive_freeze, arg)
			)

		elif isinstance(arg, set):
			return frozenset(arg)

		else:
			return arg


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


	@staticmethod
	def report_exception(logger, tier=None, info=None):
		"""
		info: dict instance to be included in the document inserted into Ampel_troubles
		"""
		# Don't create report for executions canceled manually
		if sys.exc_info()[0] == KeyboardInterrupt:
			return 

		# Feedback
		LoggingUtils.propagate_log(
			logger, logging.CRITICAL, "Exception occured", exc_info=True
		)

		# Basis document (can be appended later on)
		insert_dict = {
			'exception': traceback.format_exc().replace("\"", "'").split("\n")
		}

		# Add ampel tier (layer) info if so provided
		if tier is not None:
			insert_dict['tier'] = tier

		# Additional info might have been provided (such as alert information)
		if info is not None:
			insert_dict['more'] = info

		try:
			# Populate Ampel_trouble collection
			from ampel.pipeline.db.AmpelDB import AmpelDB
			AmpelDB.get_collection('troubles').insert_one(insert_dict)
		except:
			# Bad luck, exception again (possible cause: DB offline)
			LoggingUtils.propagate_log(
				logger,
				logging.CRITICAL, 
				"Exception occured while trying to insert document into 'troubles' collection", 
				exc_info=True
			)
