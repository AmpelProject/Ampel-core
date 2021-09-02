#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/mappings.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 17.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, List, Union, Optional, Sequence, \
	Callable, Iterable, Mapping, MutableMapping
from pydantic import BaseModel
from ampel.types import strict_iterable, T


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


def set_by_path(
	d: Dict, path: Union[str, Sequence[str]], val: Any,
	delimiter: str = '.', create: bool = True
) -> bool:
	"""
	:param create: whether to create directory sub-structures if they do not exits
	(in this case, this method will alawys return False)
	:returns: False if the key was successfully set, True otherwise
	"""
	if isinstance(path, str):
		path = path.split(delimiter) # type: ignore
	l = len(path) - 1
	for i, k in enumerate(path):
		if k not in d:
			if not create:
				return True
			d[k] = {}
		if i == l:
			d[k] = val
			return False
		d = d[k]
	return True


def del_by_path(d: Dict, path: Union[str, Sequence[str]], delimiter: str = '.') -> bool:
	""" :returns: False if the key was successfully deleted, True otherwise """

	if isinstance(path, str):
		path = path.split(delimiter) # type: ignore
	l = len(path) - 1
	for i, k in enumerate(path):
		if k not in d:
			return True
		if i == l:
			del d[k]
			return False
		d = d[k]
	return True


def walk_and_process_dict(
	arg: Union[dict, list], callback: Callable,
	match: Optional[List[str]] = None, path: str = None, **kwargs
) -> bool:
	"""
	callback is called with 4 arguments:
	1) the path of the possibly nested entry. Ex: 'processor.config.select' or 'processor'
	2) the matching key (from list 'match'). Ex: 'config'
	3) the matching (sub) dict
	4) the **kwargs provided to this method
	and should return True if a modification was performed

	Simplest callback function:
	def my_callback(path, k, d):
		print(f'{path} -> {k}: {d}\n')
		return False

	:returns: True if a modification was performed, False otherwise
	"""

	ret = False

	if isinstance(arg, list):
		for i, el in enumerate(arg):
			ret = walk_and_process_dict(
				el, callback, match, f'{path}.{i}' if path else f'{i}', **kwargs
			) or ret

	if isinstance(arg, dict):

		for k, v in arg.items():

			if not match or k in match:
				ret = callback(path, k, arg, **kwargs) or ret

			if isinstance(v, dict):
				ret = walk_and_process_dict(
					v, callback, match, f'{path}.{k}' if path else f'{k}', **kwargs
				) or ret

			if isinstance(v, list):
				for i, el in enumerate(v):
					ret = walk_and_process_dict(
						el, callback, match, f'{path}.{k}.{i}' if path else f'{k}.{i}', **kwargs
					) or ret

	return ret


def flatten_dict(
	d: Mapping,
	separator: str = '.',
	sort_keys: bool = False,
	flatten_list_members: bool = False,
	flatten_lists: bool = False,
	sort_lists: bool = False
) -> MutableMapping:
	"""
	This function is useful, among other things, for building "hash ids" of serializable dicts

	:param separator: character to be used to concatenate dict keys of different levels: {'a': {'b': 1}} -> {'a.b': 1}
	:param sort_keys: whether to sort dict keys. This applies to all dicts regardless of their depth/nesting
	:param flatten_list_members: whether to flatten dict structures embedded in list/sequences
	:param flatten_lists: whether to flatten lists, effectively converting ['a', 'b'] into {'0': 'a', '2': 'b'}
	:param sort_lists: whether to sort lists when possible, effectively converting ['r', 'a', 4, 1] into [1, 4, 'a', 'r']

	Example:
	Simplest case:
	In []: flatten_dict({'count': {'chans': {'HU_SN': 10}}})
	Out[]: {'count.chans.HU_SN': 10}

	In []: flatten_dict({'d': {'e':1}, 'a': [{'c':2}, {'b':{'f':[3, 1, 2]}}]}, sort_keys=True)
	Out[]: {'a': [{'c': 2}, {'b': {'f': [3, 1, 2]}}], 'd.e': 1}

	In []: flatten_dict({'d': {'e':1}, 'a': [{'c':2}, {'b':{'f':[3, 1, 2]}}]}, sort_keys=True, flatten_list_members=True)
	Out[]: {'a': [{'c': 2}, {'b.f': [3, 1, 2]}], 'd.e': 1}

	In []: flatten_dict({'d': {'e':1}, 'a': [{'c':2}, {'b':{'f':[3, 1, 2]}}]}, sort_keys=True, flatten_list_members=True, sort_lists=True)
	Out[]: {'a': [{'b.f': [1, 2, 3]}, {'c': 2}], 'd.e': 1}

	In []: flatten_dict({'d': {'e':1}, 'a': [{'b':{'f': [1, 2, 3]}}, {'c':2}]}, sort_keys=True, flatten_list_members=True, sort_lists=True)
	Out[]: {'a': [{'b.f': [1, 2, 3]}, {'c': 2}], 'd.e': 1}

	In []: flatten_dict({'d': {'e':1}, 'a': [{'b':{'f': [1, 2, 3]}}, {'c':2}]}, sort_keys=True, flatten_list_members=True, sort_lists=True, flatten_lists=True)
	Out[]: {'a.0.b.f.0': 1, 'a.0.b.f.1': 2, 'a.0.b.f.2': 3, 'a.1.c': 2, 'd.e': 1}

	In []: flatten_dict({'d': {'e':1}, 'a': [{'c':2}, {'b':{'f':[3, 1, 2]}}]}, sort_keys=True, flatten_list_members=True, sort_lists=True, flatten_lists=True)
	Out[]: {'a.0.b.f.0': 1, 'a.0.b.f.1': 2, 'a.0.b.f.2': 3, 'a.1.c': 2, 'd.e': 1}
	"""

	try:
		out = {}
		for k in sorted(d.keys()) if sort_keys else d:

			v = d[k]

			if isinstance(v, dict):
				for kk, vv in flatten_dict(
					v, separator, sort_keys, flatten_list_members, flatten_lists, sort_lists
				).items():
					out[f'{k}{separator}{kk}'] = vv

			elif isinstance(v, strict_iterable):

				if flatten_list_members:
					v = [
						flatten_dict(el, separator, sort_keys, flatten_list_members, flatten_lists, sort_lists)
						if isinstance(el, dict) else el
						for el in v
					]

				if sort_lists:

					try:
						# allow int/str mixed up
						v = sorted(v, key=lambda x: str(x))
					except Exception:
						pass

					# In []: sorted([{'c': 2}, {'b.f.0': 1, 'b.f.1': 2, 'b.f.2': 3}], key=lambda x: next(iter(x.keys())))
					# Out[]: [{'b.f.0': 1, 'b.f.1': 2, 'b.f.2': 3}, {'c': 2}]
					if flatten_list_members and all(isinstance(el, dict) for el in v):
						v = sorted(v, key=lambda x: next(iter(x.keys())))

				if flatten_lists:
					for kk, vv in flatten_dict(
						{i: v[i] for i in range(len(v))},
						separator, sort_keys, flatten_list_members, flatten_lists, sort_lists
					).items():
						out[f'{k}{separator}{kk}'] = vv

				else:
					out[k] = v
			else:
				out[k] = v

		return out

	except Exception as e:
		raise ValueError(f"Offending input: {d}") from e


def unflatten_dict(d: Mapping[str, Any], separator: str = '.', unflatten_list: bool = False) -> MutableMapping[str, Any]:
	"""
	Example:

	In []: unflatten_dict({'count.chans.HU_SN': 10})
	Out[]: {'count': {'chans': {'HU_SN': 10}}}

	In []: unflatten_dict({'a.0.b.f.0': 1, 'a.0.b.f.1': 2, 'a.0.b.f.2': 3, 'a.1.c': 2, 'd.e': 1}, unflatten_list=True)
	Out[]: {'a': [{'b': {'f': [1, 2, 3]}}, {'c': 2}], 'd': {'e': 1}}
	"""
	out: Dict[str, Any] = {}

	for key, value in d.items():

		parts = key.split(separator)
		target: Dict[str, Any] = out

		for part in parts[:-1]:
			if part not in target:
				target[part] = {}
			target = target[part]

		target[parts[-1]] = value

	if unflatten_list:
		return _unflatten_lists(out)

	return out


def _unflatten_lists(d: Dict) -> Dict:
	"""
	Note: modifies dict

	In []: _unflatten_lists({'a': {'0': {'b': {'f': {'0': 1, '1': 2, '2': 3}}}, '1': {'c': 2}}, 'd': {'e': 1}})
	Out[]: {'a': [{'b': {'f': [1, 2, 3]}}, {'c': 2}], 'd': {'e': 1}}
	"""

	for k, v in d.items():
		try:
			# Following line's purpose is just to trigger an error when needed:
			# it only works if v is a dict whose keys are integer (all of them)
			[int(kk) for kk in v]
			d[k] = [
				_unflatten_lists(d[k][kk]) if isinstance(d[k][kk], dict) else d[k][kk]
				for kk in v
			]
		except Exception:
			if isinstance(v, dict):
				d[k] = _unflatten_lists(v)

	return d


def merge_dict(d1: Dict, d2: Dict) -> Dict:
	k1 = set(d1.keys())
	k2 = set(d2.keys())
	return {k: d1[k] for k in k1.difference(k2)} | {k: d2[k] for k in k2.difference(k1)} | {
		k: merge_dict(d1[k], d2[k]) if isinstance(d1[k], dict) else d2[k]
		for k in k1.intersection(k2)
	}


def dictify(item):
	"""
	Recursively dictifies input
	"""
	if isinstance(item, BaseModel):
		item = item.dict(exclude_unset=False)

	if isinstance(item, dict):
		# cast potential dict subclasses into plain old dicts
		return {k: dictify(v) for k, v in item.items()}

	if isinstance(item, list):
		return [dictify(v) for v in item]

	return item


def merge_dicts(items: Sequence[Optional[Dict[T, Any]]]) -> Optional[Dict[T, Any]]:
	"""
	Merge a sequence of dicts recursively. Elements that are None are skipped.
	"""
	left = None
	for right in items:
		if left and right:
			left = merge_dict(left, right)
		elif right or left is None:
			left = right
	return left


def compare_dict_values(d1: Dict, d2: Dict, keys: Iterable[str]) -> bool:
	"""
	:returns: true if the values of dict one and two are equal for all keys requested
	Note: dict keys absent in both dicts mean that both dicts are equals wrt the dict key.

	In []: compare_dict_values({'a': 1}, {'b': 1}, ['a'])
	Out[56]: False

	In []: compare_dict_values({'a': 1}, {'a': 2}, ['a'])
	Out[]: False

	In []: compare_dict_values({'a': 1}, {'a': 1}, ['a'])
	Out[]: True

	In []: compare_dict_values({'a': 1}, {'a': 1}, ['a', 'b'])
	Out[]: True
	"""

	for f in keys:
		if f in d1:
			if f in d2:
				if d1[f] != d2[f]:
					return False
			else:
				return False
		else:
			if f in d2:
				return False
	return True


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
