#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/hash.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.05.2021
# Last Modified Date: 22.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, xxhash
from typing import Type, TypeVar, Optional, Dict
from ampel.util.mappings import flatten_dict

HT = TypeVar("HT", int, bytes, str)
xxfunc = {bytes: 'digest', int: 'intdigest', str: 'hexdigest'}

def hash_payload(payload: bytes, ret: Type[HT] = int, size: int = -64) -> HT: # type: ignore[assignment]
	"""
	:param ret: return type, can be bytes, str (hex digest) or int
	:param size: xxhash size. Available sizes: 32, 64, 128 bits.
	Convention: a negative value means the resulting int will be converted to signed int
	(mongodb does not support unsigned integers)

	FYI: using an english dict of 466544 words, hashing using the following algs resulted in:
	64 bits:
	0 collision using xxh64

	32 bits:
	23 collisions using xxh32
	25 collisions with 4-bytes truncated sha1,
	27 collisions with pyhash.fnv1a_32,
	34 collisions with 4-bytes truncated hashlib.blake2b[4bytes]

	In []: hasher = getattr(pyhash, 'xx_32')()
	In []: f=open("words.txt", "r").readlines()
	In []: len(f)
	Out[]: 466544
	In []: s=set()
	In []: for el in f:
				s.add(hasher(el))
	In []: len(f) - len(s)
	Out[]: 23
	"""

	x = getattr(xxhash, f'xxh{abs(size)}_{xxfunc[ret]}')(payload)

	# Convert unsigned to signed int if passed 'size' parameter is negative
	if size < 0 and ret == int and x & (1 << (-size-1)): # type: ignore[operator]
		x = x - 2**-size

	return x


def build_unsafe_dict_id(
	dict_arg: Optional[Dict],
	ret: Type[HT] = int, # type: ignore[assignment]
	size: int = -64,
	sort_keys: bool = True,
	flatten_list_members: bool = True,
	flatten_lists: bool = True,
	sort_lists: bool = False
) -> HT:
	"""
	:param dict_arg: can be nested, can be None
	:param ret: return type, can be bytes, str (hex digest) or int
	:param alg: hash algorithm (default is xx_64)
	:param sort_keys: see `flatten_dict` docstring
	:param flatten_list_members: see `flatten_dict` docstring
	:param sort_lists: see `flatten_dict` docstring
	:param flatten_lists: see `flatten_dict` docstring

	Examples:
	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 4]}}, size=-32)
	Out[]: -1226665093

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 4]}}, size=32)
	Out[]: 3068302203

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 4]}}, size=-64)
	Out[]: 7919378396208456243

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'a': ['r', 1, 4], 'b': 3}}, size=32, ret=str)
	Out[]: '909d2172'

	In []: build_unsafe_dict_id({'b': 2, 'a': 1, 'c': {'b': 3, 'a': [4, 'r', 1]}}, size=32, ret=str)
	Out[]: '909d2172'

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 5]}}, size=32, ret=str)
	Out[]: 'd87f695f'

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 4]}}, size=128, ret=str)
	Out[]: '747ddf83cb323db7365d303faa1bddad'

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 4]}}, size=128)
	Out[]: 154844018037484193130281437272915828141

	Note:
	In []: flatten_dict(
		{'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, {'d': 1, 'a':1}]}},
		flatten_list_members=True, flatten_lists=True, sort_keys=True
	)
	Out[]: {'a': 1, 'b': 2, 'c.a.0': 1, 'c.a.1.a': 1, 'c.a.1.d': 1, 'c.b': 3}
	"""

	if dict_arg is None:
		dict_arg = {}

	return hash_payload(
		bytes(
			json.dumps(
				flatten_dict(
					dict_arg, '.', sort_keys, flatten_list_members,
					sort_lists, flatten_lists
				),
				indent=None, separators=(',', ':')
			),
			"utf8"
		),
		ret = ret,
		size = size
	)
