#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

import warnings
from types import MappingProxyType
_global_config = None

def freeze(collection):
	"""
	Return an immutable shallow copy
	:param collection: a collection that was json serializable (i.e. consists of dicts and lists)
	"""
	if isinstance(collection, dict):
		return MappingProxyType({freeze(k): freeze(v) for k,v in collection.items()})
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
