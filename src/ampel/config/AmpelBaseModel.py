#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/AmpelModelExtension.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 01.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.ValidationError import ValidationError
from pydantic import BaseModel, BaseConfig
import logging

log = logging.getLogger(__name__)

def to_camel_case(arg: str) -> str:
	"""
	Converts snake_case to camelCase
	:returns: str
	"""
	s = arg.split("_")

	if len(s) == 1:
		return arg
	
	return s[0] + ''.join(
		word.capitalize() for word in s[1:]
	)

# TODO: rename to AmpelBaseSchema
class AmpelModelExtension(BaseModel):

	class Config(BaseConfig):
		"""
		Raise validation errors if extra fields are present,
		allows camelCase members
		"""
		allow_extra = False
		ignore_extra = False
		allow_population_by_alias = True
		alias_generator = to_camel_case


	def get(self, path):
		return AmpelUtils.get_nested_attr(self, path)


	def immutable(self) -> None:
		self.recursive_lock(self)


	@classmethod
	def recursive_lock(cls, model: BaseModel) -> None:
		""" """
		model.Config.allow_mutation=False
		for key in model.fields.keys():
			value = getattr(model, key)
			if isinstance(value, BaseModel):
				cls.recursive_lock(value)


	@classmethod
	def print_and_raise(cls, msg, header=None):
		"""
		Prints a msg and raises a ValueError with the same msg.
		Main use: sometimes, pydantic ValueError do not propagate properly
		and secondary Exceptions occur. 
		Printing the msg helps troubleshooting bad configurations.
		"""

		len_msg=0
		output="\n\n"
		for el in msg.split('\n'):
			if len(el) > len_msg:
				len_msg = len(el)

		if header:
			output += "#"*len(header)
			output += "\n" + header
			if len(msg.split('\n')) > 1:
				output += "\n" + "-"*len(header)
		else:
			output += "#"*len_msg

		output += "\n" + msg
		output += "\n" + "#"*len_msg + "\n"

		log.error(output)
		raise ValidationError(output) from None
