#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/collections.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 23.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from collections.abc import Sequence as sequence, Iterable as iterable, Sized as sized
from typing import Any, List, Iterable, Union, Type, Tuple, Set, Generator, Literal, overload
from itertools import islice
from ampel.types import strict_iterable, StrictIterable, T


def ampel_iter(arg: Any) -> Any:
	"""
	-> suppresses python3 treatment of str as iterable (a really dumb choice)
	-> Makes None iterable
	"""
	return [arg] if isinstance(arg, (type(None), str, int, bytes, bytearray)) else arg


def chunks(seq: Iterable[T], n: int) -> Generator[List[T], None, None]:
	"""
	Yield chunks of length `n` from `seq`
	"""
	source = iter(seq)
	while True:
		if chunk := list(islice(source, n)):
			yield chunk
			if len(chunk) < n:
				break
		else:
			break


def try_reduce(arg: Any) -> Any:
	"""
	Returns element contained by sequence if sequence contains only one element.
	Example:
	try_reduce(['ab']) -> returns 'ab'
	try_reduce({'ab'}) -> returns 'ab'
	try_reduce('ab') -> returns 'ab'
	try_reduce(['a', 'b']) -> returns ['a', 'b']
	try_reduce({'a', 'b'}) -> returns {'a', 'b'}
	try_reduce(dict(a=1).keys()) -> returns 'a'
	try_reduce(dict(a=1, b=1).keys()) -> returns dict_keys(['a', 'b'])
	"""

	if isinstance(arg, sized) and len(arg) == 1:
		if isinstance(arg, sequence):
			return arg[0]
		return next(iter(arg)) # type: ignore

	return arg


@overload
def merge_to_list(
	arg1: Union[str, int, List[Union[str, int]], Set[Union[str, int]]],
	arg2: Union[str, int, List[Union[str, int]], Set[Union[str, int]]],
	reduce: Literal[True]
) -> Union[str, int, List[Union[str, int]], Set[Union[str, int]]]:
	...

@overload
def merge_to_list(
	arg1: Union[str, int, List[Union[str, int]], Set[Union[str, int]]],
	arg2: Union[str, int, List[Union[str, int]], Set[Union[str, int]]],
	reduce: Literal[False]
) -> Union[List[Union[str, int]], Set[Union[str, int]]]:
	...

def merge_to_list(
	arg1: Union[str, int, List[Union[str, int]], Set[Union[str, int]]],
	arg2: Union[str, int, List[Union[str, int]], Set[Union[str, int]]],
	reduce: bool = True
) -> Union[str, int, List[Union[str, int]], Set[Union[str, int]]]:
	""" Returns a merged list unless arg1 and arg2 have the same scalar value """

	if isinstance(arg1, (str, int)):
		if isinstance(arg2, (str, int)):
			if arg2 != arg1:
				return [arg1, arg2]
			return arg1 if reduce else [arg1]
		else:
			if arg1 in arg2:
				return arg2 if isinstance(arg2, list) else list(arg2)
			l = list(arg2)
			l.append(arg1)
			return l

	# arg1 is a list/set
	else:
		if isinstance(arg2, (int, str)):
			if arg2 in arg1:
				return arg1 if isinstance(arg1, list) else list(arg1)
			l = list(arg1)
			l.append(arg2)
			return l
	
		# arg1 and arg2 are list/set
		else:
			return list(
				(arg1 if isinstance(arg1, set) else set(arg1)) |
				(arg2 if isinstance(arg2, set) else set(arg2))
			)


def to_set(arg) -> Set:
	"""
	Reminder of python questionable logic:
	In []: set('abc')
	Out[]: {'a', 'b', 'c'}
	In []: {'abc'}
	Out[]: {'abc'}

	In []: to_set("abc")
	Out[]: {'abc'}
	In []: to_set(["abc"])
	Out[]: {'abc'}
	In []: to_set(['a','b','c'])
	Out[]: {'a', 'b', 'c'}
	In []: to_set([1,2])
	Out[]: {1, 2}
	"""
	return set(arg) if isinstance(arg, strict_iterable) else {arg}


def to_list(arg: Union[int, str, bytes, bytearray, List, Iterable]) -> List:
	"""
	raises ValueError is arg is not int, str, bytes, bytearray, list, or Iterable
	"""
	if isinstance(arg, (int, str, bytes, bytearray)):
		return [arg]
	if isinstance(arg, list):
		return arg
	if isinstance(arg, iterable):
		return list(arg)

	raise ValueError(f"Unsupported argument type ({type(arg)})")


def check_seq_inner_type(
	seq, types: Union[Type, Tuple[Type, ...]], multi_type: bool = False
) -> bool:
	"""
	check type of all elements contained in a sequence.
	*all* members of the provided sequence must match with:
		* multi_type == False: one of the provided type.
		* multi_type == True: any of the provided type.

	check_seq_inner_type((1,2), int) -> True
	check_seq_inner_type([1,2], int) -> True
	check_seq_inner_type((1,2), float) -> False
	check_seq_inner_type(('a','b'), str) -> True
	check_seq_inner_type((1,2), (int, str)) -> True
	check_seq_inner_type((1,2,'a'), (int, str)) -> False
	check_seq_inner_type((1,2,'a'), (int, str), multi_type=True) -> True

	Note:
	check_seq_inner_type('dsda', str) -> True
	check_seq_inner_type(23, int) -> False
	"""

	# Wrong input
	if not isinstance(seq, sequence) or isinstance(seq, str):
		return False

	# monotype
	if not isinstance(types, sequence):
		return all(isinstance(el, types) for el in seq)

	# different types accepted ('or' connected)
	if multi_type:
		return all(isinstance(el, types) for el in seq)

	return any(
		tuple(check_seq_inner_type(seq, _type) for _type in types)
	)


def has_nested_type(obj: StrictIterable, target_type: Type, strict: bool = True) -> bool:
	"""
	:param obj: object instance (dict/list/set/tuple)
	:param target_type: example: ReadOnlyDict/list
	:param strict: must be an instance of the provided type (subclass instances would be rejected)
	"""

	if strict:
		# pylint: disable=unidiomatic-typecheck
		if type(obj) is target_type:
			return True
	else:
		if isinstance(obj, target_type):
			return True

	if isinstance(obj, dict):
		for el in obj.values():
			if has_nested_type(el, target_type):
				return True

	elif isinstance(obj, strict_iterable):
		for el in obj:
			if has_nested_type(el, target_type):
				return True

	return False
