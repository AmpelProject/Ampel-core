#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/AmpelFlexModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 03.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, BaseConfig, Extra
from ampel.utils.Freeze import Freeze
from ampel.model.AmpelStrictModel import to_camel_case
from ampel.utils.AmpelUtils import AmpelUtils


class AmpelFlexModel(BaseModel):
	""" """

	class Config(BaseConfig):
		"""
		Raise validation errors if extra fields are present,
		allows camelCase members
		"""
		extra = Extra.allow
		arbitrary_types_allowed = True
		allow_population_by_field_name = True
		alias_generator = to_camel_case


	def get(self, path):
		return AmpelUtils.get_nested_attr(self, path)


	def immutable(self) -> None:
		Freeze.recursive_lock(self)
