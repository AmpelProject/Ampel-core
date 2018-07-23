#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 23.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections, logging
from functools import reduce
from types import MappingProxyType

class AmpelUtils():
	""" 
	"""

	number_map = {
		'10': 'a', '11': 'b', '12': 'c', '13': 'd', '14': 'e', '15': 'f',
		'16': 'g', '17': 'h', '18': 'i', '19': 'j', '20': 'k', '21': 'l',
		'22': 'm', '23': 'n', '24': 'o', '25': 'p', '26': 'q', '27': 'r',
		'28': 's', '29': 't', '30': 'u', '31': 'v', '32': 'w', '33': 'x',
		'34': 'y', '35': 'z'
	}

	letter_map = {
		 'a': '10', 'b': '11', 'c': '12', 'd': '13', 'e': '14', 'f': '15',
		 'g': '16', 'h': '17', 'i': '18', 'j': '19', 'k': '20', 'l': '21',
		 'm': '22', 'n': '23', 'o': '24', 'p': '25', 'q': '26', 'r': '27',
		 's': '28', 't': '29', 'u': '30', 'v': '31', 'w': '32', 'x': '33',
		 'y': '34', 'z': '35'
	}


	@staticmethod
	def get_ampel_name(ztf_name):
		"""	
		Returns an int. 
		"""

		# Handle sequences
		if type(ztf_name) in (list, tuple):
			return [AmpelUtils.get_ampel_name(name) for name in ztf_name]

		letter_map = AmpelUtils.letter_map
		return int(
			"".join(
				(	
					ztf_name[3:5], 
					letter_map[ztf_name[5]], 
					letter_map[ztf_name[6]], 
					letter_map[ztf_name[7]], 
					letter_map[ztf_name[8]], 
					letter_map[ztf_name[9]], 
					letter_map[ztf_name[10]], 
					letter_map[ztf_name[11]]
				)
			)
		)


	@staticmethod
	def get_ztf_name(db_long):
		"""	
		Returns a string. 
		"""
		# Handle sequences
		if type(db_long) in (list, tuple):
			return [AmpelUtils.get_ztf_name(l) for l in db_long]

		str_long = str(db_long)
		number_map = AmpelUtils.number_map

		return "ZTF%s%s%s%s%s%s%s%s" % (
			str_long[0:2],
			number_map[str_long[2:4]],
			number_map[str_long[4:6]],
			number_map[str_long[6:8]],
			number_map[str_long[8:10]],
			number_map[str_long[10:12]],
			number_map[str_long[12:14]],
			number_map[str_long[14:16]]
		)


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
		A sequence, but not a string
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
		import traceback

		# This kind of events need to be propagated
		logger.propagate = True

		# Get traceback as string
		exception_str = traceback.format_exc().replace("\"", "'")

		# Don't create report for executions canceled manually
		if "KeyboardInterrupt" in exception_str:
			return 

		# Feedback
		AmpelUtils.propagate_log(
			logger, logging.CRITICAL, "Exception occured", exc_info=True
		)

		# Basis document (can be appended later on)
		insert_dict = {
			'exception': exception_str.split("\n")
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
			AmpelUtils.propagate_log(
				logger,
				logging.CRITICAL, 
				"Exception occured while trying to insert document into 'troubles' collection", 
				exc_info=True
			)


	@staticmethod
	def propagate_log(logger, level, msg, exc_info=False):
		"""
		"""
		if logger.propagate is False:
			logger.propagate = True
			logger.log(level, msg, exc_info=exc_info)
			logger.propagate = False
		else:
			logger.log(level, msg, exc_info=exc_info)
