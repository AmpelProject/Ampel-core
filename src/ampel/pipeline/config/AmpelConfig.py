#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelConfig.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.06.2018
# Last Modified Date: 06.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import warnings
from ampel.pipeline.config.ReadOnlyDict import ReadOnlyDict
from ampel.pipeline.common.AmpelUtils import AmpelUtils

class AmpelConfig:


	# Static dict/ReadOnlyDict holding all ampel configurations
	_global_config = None
	_ignore_unavailable_units = set()


	@classmethod
	def initialized(cls):
		""" """ 
		return cls._global_config is not None


	@classmethod
	def ignore_unavailable_units(cls, from_tiers):
		"""
		:param from_tiers: combination of "t0", "t2", "t3".
		:type from_tiers: list(str), str
		Enables scenario such as: 
		- the AlertProcessor creates T2 documents that are processed by external ressources 
		(say on an external grid such as NERSC).
		The corresponding t2 unit(s) do not need to be present in the ampel image.
		- Ampel T3 is run on a dedicated server but a general common ampelconfig is used.
		"""
		if isinstance(from_tiers, str):
			cls._ignore_unavailable_units.add(from_tiers)
		elif AmpelUtils.is_sequence(from_tiers):
			cls._ignore_unavailable_units.update(from_tiers)
		else:
			raise ValueError("Invalid argument")


	@classmethod
	def load_defaults(cls, ignore_unavailable_units=None):
		"""
		:param set(str) ignore_unavailable_units: combination of "t0", "t2", "t3". See 
		:func:`ignore_unavailable_units <ampel.pipeline.config.AmpelConfig.ignore_unavailable_units>`
		docstring
		"""
		if ignore_unavailable_units:
			cls._ignore_unavailable_units = set(ignore_unavailable_units)
		from ampel.pipeline.config.ConfigLoader import ConfigLoader
		cls.set_config(
			ConfigLoader.load_config(gather_plugins=True)
		)


	@classmethod
	def get_config(cls, sub_element=None):
		""" 
		Optional arguments:
		:param sub_element: only sub-config element will be returned. \
		Example: get_config("channels.HU_RANDOM")
		:type sub_element: str, list
		"""
		if not cls.initialized():
			raise RuntimeError("Ampel global config not set")

		if sub_element is None:
			return cls._global_config

		sub_conf = AmpelUtils.get_by_path(cls._global_config, sub_element)
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
