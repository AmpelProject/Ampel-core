#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/AmpelStrictModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, BaseConfig, Extra
#from ampel.utils.Freeze import Freeze
#from ampel.utils.mappings import get_nested_attr


#def to_camel_case(arg: str) -> str:
#	"""
#	Converts snake_case to camelCase
#	:returns: str
#	"""
#	s = arg.split("_")
#
#	if len(s) == 1:
#		return arg
#
#	return s[0] + ''.join(
#		word.capitalize() for word in s[1:]
#	)


class AmpelStrictModel(BaseModel):
	""" """

	class Config(BaseConfig):
		"""
		Raise validation errors if extra fields are present,
		allows camelCase members
		"""
		extra = Extra.forbid
		arbitrary_types_allowed = True
		allow_population_by_field_name = True
		#alias_generator = to_camel_case


	def __init__(self, **kwargs):
		self.__config__.extra = Extra.forbid
		super().__init__(**kwargs)
		self.__config__.extra = Extra.allow


#	def get(self, path):
#		return get_nested_attr(self, path)
#
#
#	def immutable(self) -> None:
#		Freeze.recursive_lock(self)
