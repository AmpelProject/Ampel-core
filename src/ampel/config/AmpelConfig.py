#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/AmpelConfig.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.06.2018
# Last Modified Date: 04.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.ReadOnlyDict import ReadOnlyDict

class AmpelConfig:
	""" """

	@staticmethod
	def new_default(pwd_file=None, verbose=False, ignore_errors=False):
		"""
		"""
		# Import here to avoid cyclic import error
		from ampel.config.ConfigBuilder import ConfigBuilder

		ampel_config = AmpelConfig()
		cb = ConfigBuilder(verbose=verbose)
		cb.load_distributions()

		if pwd_file:
			cb.add_passwords(
				json.load(
					open(pwd_file, "r")
				)
			)

		ampel_config.set(
			cb.get_config(ignore_errors=ignore_errors)
		)

		return ampel_config


	def __init__(self):
		""" """
		self._config = {}


	def get(self, sub_element:str=None):
		""" 
		Optional arguments:
		:param str sub_element: only sub-config element will be returned. \
		Example: get("channel.HU_RANDOM")
		"""
		if not self.initialized():
			raise RuntimeError("Ampel global config not set")

		if sub_element is None:
			return self._config

		return AmpelUtils.get_by_path(
			self._config, sub_element
		)


	def print(self, sub_element: str=None) -> None:
		"""
		"""
		print(
			json.dumps(
				self.get(sub_element), indent=4
			)
		)


	def set(self, config):
		""" """
		if self._config is not None:
			import warnings
			warnings.warn("Resetting global configuration")

		self._config = AmpelConfig.recursive_freeze(config)

		return self._config


	def reset(self):
		""" """ 
		self._config = None


	def initialized(self):
		""" """ 
		return self._config is not None


	def is_frozen(self):
		""" """ 
		return isinstance(self._config, ReadOnlyDict)


	@classmethod
	def recursive_freeze(cls, arg):
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
					cls.recursive_freeze(k): cls.recursive_freeze(v) 
					for k,v in arg.items()
				}
			)

		elif isinstance(arg, list):
			return tuple(
				map(cls.recursive_freeze, arg)
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
