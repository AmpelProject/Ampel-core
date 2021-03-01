#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/mappings.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 17.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Dict, Any, List, Union, Type, Optional, Sequence, TypeVar, Literal, Callable, Iterable, Mapping, MutableMapping
from ampel.type import strict_iterable
from ampel.util.crypto import hash_payload, HT
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.util.collections import check_seq_inner_type
from ampel.util.crypto import b2_short_hash

T = TypeVar('T')

def build_unsafe_short_dict_id(dict_arg: Optional[Dict]) -> int:
	"""
	Note: no collision occured applying blake2 using 7bytes digests on the word list
	https://github.com/dwyl/english-words/blob/master/words.txt
	containing 466544 english words
	:returns: 7bytes int (MongoDB supports only *signed* 64-bit integers)

	example:
	In []: build_unsafe_short_dict_id({'a': 1})
	Out[]: 18043533495046284

	In []: build_unsafe_short_dict_id({'b': 2, 'a': 1, 'c': {'b': 3, 'a': [4, 'r', 1]}})
	Out[]: 53310293724158701

	In []: build_unsafe_short_dict_id({'a': 1, 'b': 2, 'c': {'a': [4, 1, 'r'], 'b': 3}})
	Out[]: 53310293724158701
	"""
	return build_unsafe_dict_id(dict_arg, int, 'blake2b', digest_size=7)


def build_unsafe_dict_id(
	dict_arg: Optional[Dict],
	ret: Type[HT] = bytes, # type: ignore[assignment]
	alg: Literal['sha512', 'sha1', 'blake2b'] = 'sha512',
	sort_keys: bool = True,
	flatten_list_members: bool = True,
	sort_lists: bool = True,
	flatten_lists: bool = True,
	**kwargs
) -> HT:
	"""
	:param dict_arg: can be nested, can be None
	:param ret: return type, can be bytes, str (hex digest) or int
	:param alg: hash algorithm (default is sha512)
	:param sort_keys: see `flatten_dict` docstring
	:param flatten_list_members: see `flatten_dict` docstring
	:param sort_lists: see `flatten_dict` docstring
	:param flatten_lists: see `flatten_dict` docstring
	:param kwargs: will be forwarded to hashlib hash function

	example:
	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'a': ['r', 1, 4], 'b': 3}}, ret=str)
	Out[]: 'b5acfa0d427fe1ef682895217c94400178b5700997a9547fe5bebf33b73d8157332c2bb1bd0e370
	0c8c232fd55f4993b1b34132420afc14a05e2414df3037519'

	In []: build_unsafe_dict_id({'b': 2, 'a': 1, 'c': {'b': 3, 'a': [4, 'r', 1]}}, ret=str)
	Out[]: 'b5acfa0d427fe1ef682895217c94400178b5700997a9547fe5bebf33b73d8157332c2bb1bd0e370
	0c8c232fd55f4993b1b34132420afc14a05e2414df3037519'

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 5]}}, ret=str)
	Out[]: '1f8e8c35e9641a6f8cb5a1136b712cf1b735577645db6e1ee373ea7dd08266b63a8b23fde0
	615c6916205cfdf928e42cd79171581c211eb77bed967d65563b2f'

	In []: build_unsafe_dict_id({'a': 1, 'b': 2, 'c': {'b': 3, 'a': [1, 4]}}, ret=int, alg='sha1')
	Out[]: 967659017817567346241766354309619194352380159869

	In []: build_unsafe_dict_id({'a': 1, 'b': 2}, ret=int, alg='blake2b', digest_size=7)
	Out[]: 32414584742937293
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
		ret, alg, **kwargs
	)


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
	match: List[str], path: str = None, **kwargs
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

			if k in match:
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
	return {
		**{k: d1[k] for k in k1.difference(k2)},
		**{k: d2[k] for k in k2.difference(k1)},
		**{
			k: merge_dict(d1[k], d2[k]) if isinstance(d1[k], dict) else d2[k]
			for k in k1.intersection(k2)
		}
	}


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


def hash_logic_schema(
	arg: Optional[Union[str, dict, AllOf, AnyOf, OneOf]]
) -> Union[int, dict]:
	"""
	Converts dict schema containing str representation of tags into
	a dict schema containing hashed values (int64).

	:param arg: schema dict. \
	See :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` \
	docstring for more details

	examples:
	In []: hash_logic_schema('aa')
	Out[]: 24517795197330556

	In []: hash_logic_schema({'allOf': ['aa', 'bb', 12]})
	Out[]: {'allOf': [24517795197330556, 14271023143587293, 12]}

	In []: hash_logic_schema({'anyOf': ['aa', 'bb', 12]})
	Out[]: {'anyOf': [24517795197330556, 14271023143587293, 12]}

	In []: hash_logic_schema({'anyOf': [{'allOf': ['aa', 'bb', 100]}, 'cc']})
	Out[]: {'anyOf': [{'allOf': [24517795197330556, 14271023143587293, 100]}, 59944183417054336]}

	:returns: new schema dict where tag elements are integers
	"""

	out: Dict[str, Any] = {}

	if isinstance(arg, str):
		return b2_short_hash(arg)

	if isinstance(arg, (AllOf, AnyOf, OneOf)):
		arg = arg.dict()

	if isinstance(arg, dict):

		if 'anyOf' in arg:
			if check_seq_inner_type(arg['anyOf'], str):
				out['anyOf'] = _hash_elements(arg['anyOf'])
			else:
				out['anyOf'] = []
				for el in arg['anyOf']:
					if isinstance(el, str):
						out['anyOf'].append(b2_short_hash(el))
					elif isinstance(el, dict):
						if 'allOf' not in el:
							raise ValueError('Unsupported format (1)')
						out['anyOf'].append(
							{'allOf': _hash_elements(el['allOf'])}
						)
					else:
						out['anyOf'].append(el)

		elif 'allOf' in arg:
			out['allOf'] = _hash_elements(arg['allOf'])

		elif 'oneOf' in arg:
			out['oneOf'] = _hash_elements(arg['oneOf'])
	else:
		raise ValueError(f'Unsupported argument type: "{type(arg)}"')

	return out


def _hash_elements(seq: Sequence) -> List:

	return [
		b2_short_hash(el) if isinstance(el, str) else el
		for el in seq
	]
