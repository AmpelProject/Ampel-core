#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/AmpelModelExtension.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 07.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.common.AmpelUtils import AmpelUtils

class AmpelModelExtension:

	def get(self, path):
		return AmpelUtils.get_nested_attr(self, path)

	@staticmethod	
	def print_and_raise(msg, header=None):
		"""
		Prints a msg and raises a ValueError with the same msg.
		Main use: sometimes, pydantic ValueError do not propagate properly
		and secondary Exceptions occur. 
		Printing the msg helps troubleshooting bad configurations.
		"""
		if header:
			print("#"*len(header))
			print(header)
		else:
			print("#"*len(msg))
		print(msg)
		print("#"*len(msg))
		raise ValueError(msg)
