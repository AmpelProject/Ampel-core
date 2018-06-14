#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

import warnings
_global_config = None

class frozendict(dict):
	def __repr__(self):
		return '{}({})'.format(type(self).__name__, super(type(self), self).__repr__())
	def _disable(self, *args, **kwargs):
		raise AttributeError('frozendict is immutable')
	update = _disable
	pop = _disable
	popitem = _disable
	setdefault = _disable
	clear = _disable
	__setitem__ = _disable
	__delitem__ = _disable
	__setattr__ = _disable
	__delattr__ = _disable

def freeze(collection):
	"""
	Return an immutable shallow copy
	:param collection: a collection that was json serializable (i.e. consists of dicts and lists)
	"""
	if isinstance(collection, dict):
		return frozendict({freeze(k): freeze(v) for k,v in collection.items()})
	elif isinstance(collection, list):
		return tuple(map(freeze, collection))
	else:
		return collection

class _global_config:
	pass

def set_config(config):
	if hasattr(_global_config, 'value'):
		warnings.warn("Resetting global configuration")
	_global_config.value = freeze(config)
	return _global_config.value

def get_config():
	return _global_config.value
