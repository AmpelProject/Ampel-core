#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelConfig.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.06.2018
# Last Modified Date: 24.07.2018
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
		cls._global_config = AmpelUtils.recursive_freeze(config)
		return cls._global_config


	@classmethod
	def is_frozen(cls):
		""" """ 
		return type(cls._global_config) is ReadOnlyDict
