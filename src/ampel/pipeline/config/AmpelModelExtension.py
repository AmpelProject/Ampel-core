#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelModelExtension.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 07.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.common.AmpelUtils import AmpelUtils
from pydantic import BaseModel, BaseConfig

class AmpelModelExtension(BaseModel):

	class Config(BaseConfig):
		"""
		Raise validation errors if extra fields are present
		"""
		allow_extra = False
		ignore_extra = False

	class ValidationError(Exception):
		"""
		We use our own ValidationError because pydantic catches ValueErrors a-like 
		expressions in order to build a summary of validation errors.
		Pblm is: this summary is hardly understandable and distracts attention 
		from the root cause of the pblm. We could rise a - say RunTimeError - 
		but this would be misleading since it is a validation problem. 
		So we create our own ValidationError (that is not caught by pydantic like ValueError) 
		which as a consequence breaks the chain of validation as soon as it is raised.
		"""
		pass

	def get(self, path):
		return AmpelUtils.get_nested_attr(self, path)


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

		print(output)
		raise cls.ValidationError(output) from None
