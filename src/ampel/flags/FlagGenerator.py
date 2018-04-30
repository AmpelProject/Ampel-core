#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/FlagGenerator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.03.2018
# Last Modified Date: 30.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntFlag
from types import MethodType

class FlagGenerator:
	"""
	DEPRECATED
	"""

	ChannelFlags = None
	T2UnitIds = None


	@classmethod	
	def get_ChannelFlags_class(cls, mongo_collection, force_create=False):
		"""
		DEPRECATED
		"""
		# Check for previously generated enum flag
		prev_class = getattr(FlagGenerator, 'ChannelFlags', None)

		if prev_class is None or force_create:
			cls.ChannelFlags = FlagGenerator.create_class(
				mongo_collection, "_id", 'ChannelFlags', listable=True
			)

		return cls.ChannelFlags
	

	@classmethod	
	def get_T2UnitIds_class(cls, mongo_collection, force_create=False):
		"""
		DEPRECATED
		"""
		# Check for previously generated enum flag
		prev_class = getattr(FlagGenerator, 'T2UnitIds', None)

		if prev_class is None or force_create:
			cls.T2UnitIds = FlagGenerator.create_class(
				mongo_collection, "_id", 'T2UnitIds', listable=True
			)

		return cls.T2UnitIds
		

	@staticmethod	
	def create_class(
		mongo_collection, field_name, class_name, listable=False
	):
		"""
		DEPRECATED
		"""

		class_member = [el[field_name] for el in mongo_collection.find({})]

		# Following pylint error report is wrong:
		# E: 20,15: Unexpected keyword argument 'names' in constructor call (unexpected-keyword-arg)
		FlagClass = IntFlag(class_name, names=class_member) 

		if listable:
			FlagClass.as_list = as_list

		return FlagClass


def as_list(self):
	return [el for el in self.__class__ if el in self]

