#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelConfig.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.06.2018
# Last Modified Date: 16.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import warnings
from types import MappingProxyType
from ampel.pipeline.common.AmpelUtils import AmpelUtils

class AmpelConfig:
	

	# path relative the AmpelConfig path (<Ampel base dir>/src/ampel/pipeline/config
	_fallback_config_path = '../../../../config/ampel.config'
	_global_config = None
	_dev_mode = False


	@classmethod
	def initialized(cls):
		return cls._global_config is not None


	@classmethod
	def _auto_initialize(cls):
		try:
			import json, os
			cls.set_config(
				json.load(open("%s/%s" % (
					os.path.dirname(os.path.abspath(__file__)),
					cls._fallback_config_path), 'r'
				))
			)

		except Exception as e:
			print("Auto-initialization failed")
			print(e)
			# TODO: try something else ?
			pass


	@classmethod
	def _developer_mode(cls):
		cls._dev_mode = True


	@classmethod
	def get_config(cls, key=None, validate=None):
		""" 
		Optional arguments:
		'key' -> only sub-config elements with provided key will be returned.
		    Example: get_config("channels.HU_RANDOM")
		'validate' -> optional instance of voluptous schema. Validation will only work:
			* if config is not frozen since frozen configs are created using validation
			* if key is provided since only parts of the global config follow given schemas
		"""
		if cls._global_config is None:
			# try to auto initialize
			cls._auto_initialize()
			if cls._global_config is None:
				raise ValueError("Ampel global config not set")

		if key is None:
			return cls._global_config

		sub_conf = AmpelUtils.get_by_path(cls._global_config, key)
		if sub_conf is None:
			return sub_conf
			
		return sub_conf if (validate is None or cls.is_frozen()) else validate(sub_conf)

	
	@classmethod
	def set_config(cls, config):
		""" """
		if cls._global_config is not None:
			warnings.warn("Resetting global configuration")
		cls._global_config = config if cls._dev_mode else AmpelConfig.freeze(config)
		return cls._global_config


	@classmethod
	def is_frozen(cls):
		return type(cls._global_config) is MappingProxyType
	

	@classmethod
	def freeze(cls, collection):
		"""
		Return an immutable shallow copy
		:param collection: a collection that was json serializable (i.e. consists of dicts and lists)
		"""
		if isinstance(collection, dict):
			return MappingProxyType(
				{cls.freeze(k): cls.freeze(v) for k,v in collection.items()}
			)
		elif isinstance(collection, list):
			return tuple(map(cls.freeze, collection))
		else:
			return collection
