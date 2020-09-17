#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/utils/collections.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from collections.abc import Sequence as sequence, Iterable as iterable, Sized as sized
from typing import Dict, Any, List, Iterable, Union, Type, Tuple, Set, Sized, Generator
from itertools import islice
from ampel.type import strict_iterable


def ampel_iter(arg: Any) -> Any:
	"""
	-> suppresses python3 treatment of str as iterable (a really dumb choice)
	-> Makes None iterable
	"""
	return [arg] if isinstance(arg, (type(None), str, int, bytes, bytearray)) else arg


def chunks(seq: Iterable, n: int) -> Generator[List, None, None]:
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
	try_reduce(['a', 'b']) -> returns {'a', 'b'}
	try_reduce(dict(a=1).keys()) -> returns 'a'
	try_reduce(dict(a=1, b=1).keys()) -> returns dict_keys(['a', 'b'])
	"""

	if isinstance(arg, sized) and len(arg) == 1:
		if isinstance(arg, sequence):
			return arg[0]
		return next(iter(arg)) # type: ignore

	return arg


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


def to_list(cls, arg: Union[int, str, bytes, bytearray, List, Iterable]) -> List:
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
	"""

	# monotype
	if not isinstance(types, sequence):
		return all(isinstance(el, types) for el in seq)

	# different types accepted ('or' connected)
	if multi_type:
		return all(isinstance(el, types) for el in seq)

	return any(
		tuple(check_seq_inner_type(seq, _type) for _type in types)
	)
