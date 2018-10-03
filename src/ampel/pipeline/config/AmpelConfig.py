#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelConfig.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.06.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import warnings
from ampel.pipeline.config.ReadOnlyDict import ReadOnlyDict
from ampel.pipeline.common.AmpelUtils import AmpelUtils

class AmpelConfig:


	# Static dict/ReadOnlyDict holding all ampel configurations
	_global_config = None


	@classmethod
	def initialized(cls):
		""" """ 
		return cls._global_config is not None


	@classmethod
	def load_defaults(cls):
		from ampel.pipeline.config.ConfigLoader import ConfigLoader
		cls.set_config(
			ConfigLoader.load_config(gather_plugins=True)
		)

	@classmethod
	def get_config(cls, key=None):
		""" 
		Optional arguments:
		'key' -> only sub-config elements with provided key will be returned.
		    Example: get_config("channels.HU_RANDOM")
		"""
		if not cls.initialized():
			raise RuntimeError("Ampel global config not set")

		if key is None:
			return cls._global_config

		sub_conf = AmpelUtils.get_by_path(cls._global_config, key)
		if sub_conf is None:
			return sub_conf
			
		return sub_conf


	@classmethod
	def reset(cls):
		""" """ 
		cls._global_config = None
	

	@classmethod
	def set_config(cls, config):
		""" """
		if cls._global_config is not None:
			warnings.warn("Resetting global configuration")
		cls._global_config = AmpelConfig.recursive_freeze(config)
		return cls._global_config


	@classmethod
	def is_frozen(cls):
		""" """ 
		return type(cls._global_config) is ReadOnlyDict


	@staticmethod
	def recursive_freeze(arg):
		"""
		Return an immutable shallow copy
		:param arg:
			dict: ReadOnlyDict is returned
			list: tuple is returned
			set: frozenset is returned
			otherwise: arg is returned 'as is'
		"""
		if isinstance(arg, dict):
			return ReadOnlyDict(
				{
					AmpelConfig.recursive_freeze(k): AmpelConfig.recursive_freeze(v) 
					for k,v in arg.items()
				}
			)

		elif isinstance(arg, list):
			return tuple(
				map(AmpelConfig.recursive_freeze, arg)
			)

		elif isinstance(arg, set):
			return frozenset(arg)

		else:
			return arg


	@classmethod
	def recursive_unfreeze(cls, arg):
		"""
		Inverse of AmpelConfig.recursice_freeze
		"""
		if isinstance(arg, ReadOnlyDict):
			return dict(
				{
					cls.recursive_unfreeze(k): cls.recursive_unfreeze(v) 
					for k,v in arg.items()
				}
			)

		elif isinstance(arg, tuple):
			return list(
				map(cls.recursive_unfreeze, arg)
			)

		elif isinstance(arg, frozenset):
			return set(arg)

		else:
			return arg


	@staticmethod
	def has_nested_type(obj, target_type):
		"""
		:param obj: object instance (dict/list/set/tuple)
		:param type target_type: example: ReadOnlyDict/list
		"""
		if type(obj) is target_type:
			return True

		if isinstance(obj, dict):
			for el in obj.values():
				if AmpelConfig.has_nested_type(el, target_type):
					return True

		elif AmpelUtils.is_sequence(obj):
			for el in obj:
				if AmpelConfig.has_nested_type(el, target_type):
					return True

		return False
