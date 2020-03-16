#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/utils/mapping.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, hashlib, sys
from typing import Dict, Any, List, Union, Type, Optional, Sequence, overload


def build_unsafe_dict_id(dict_arg: Optional[Dict]) -> str:
	"""
	Unsafe because 1) SHA1 is used 2) only a subset of the hex digest is used.
	However, it should be sufficiently safe for many usecases
	Note: None dict_arg returns a hash since json.dumps returns "null" in this case
	:param dict_arg: can be nested, can be None
	:returns: short dict id (ex: 'b2202f') made of a the last 6 characters of a SHA1 hex string.
	"""
	return hashlib.sha1(
		bytes(
			json.dumps(
				dict_arg, sort_keys=True,
				indent=None, separators=(',', ':')
			),
			"utf8"
		)
	).hexdigest()[-6:]


@overload
def build_safe_dict_id(dict_arg: Optional[Dict], ret: Type[bytes]) -> bytes:
	...

@overload
def build_safe_dict_id(dict_arg: Optional[Dict], ret: Type[str]) -> str:
	...

@overload
def build_safe_dict_id(dict_arg: Optional[Dict], ret: Type[int]) -> int:
	...

def build_safe_dict_id(
	dict_arg: Optional[Dict], ret: Type[Union[bytes, str, int]] = bytes
) -> Union[bytes, str, int]:
	"""
	Takes ~7µs on a MBP 2017 for a small dict
	:param dict_arg: can be nested, can be None
	:returns: SHA512 hex string.
	"""
	ho = hashlib.sha512(
		bytes(
			json.dumps(
				dict_arg, sort_keys=True,
				indent=None, separators=(',', ':')
			),
			"utf8"
		)
	)

	if ret == bytes:
		return ho.digest()

	if ret == int:
		return int.from_bytes(
			ho.digest(), byteorder=sys.byteorder
		)

	return ho.hexdigest()


def get_by_path(
	mapping: Dict, path: Union[str, Sequence[str]], delimiter: str = '.'
) -> Optional[Any]:
	"""
	Get an item from a nested mapping by path, e.g.
	'foo.bar.baz' -> mapping['foo']['bar']['baz']

	:param path: example: 'foo.bar.baz' or ['foo', 'bar', 'baz']
	:param delimiter: example: '.'
	"""

	if isinstance(path, str):
		path = path.split(delimiter)

	# check for int elements encoded as str
	path: List[Union[int, str]] = [ # type: ignore
		(el if not el.isdigit() else int(el)) for el in path
	]

	for el in path:
		if el not in mapping:
			return None
		mapping = mapping[el]

	return mapping


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


def flatten_dict(d: Dict, separator: str = '.') -> Dict:
	"""
	Example:
	input: {'count': {'chans': {'HU_SN': 10}}}
	output: {'count.chans.HU_SN': 10}
	"""
	expand = lambda key, val: (
		[(key + separator + k, v) for k, v in flatten_dict(val).items()]
		if isinstance(val, dict) else [(key, val)]
	)

	items = [item for k, v in d.items() for item in expand(k, v)]

	return dict(items)


def unflatten_dict(d: Dict, separator: str = '.') -> Dict:
	"""
	Example:
	input: {'count.chans.HU_SN': 10}
	output: {'count': {'chans': {'HU_SN': 10}}}
	Note: this method does not work recursively
	"""
	res: Dict = {}

	for key, value in d.items():

		parts = key.split(separator)
		d = res

		for part in parts[:-1]:
			if part not in d:
				d[part] = {}
			d = d[part]

		d[parts[-1]] = value

	return res
