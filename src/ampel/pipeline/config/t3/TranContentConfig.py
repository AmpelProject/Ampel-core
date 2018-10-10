#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/TranContentConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator, ValidationError
from typing import Dict, Union, List, Any
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.config.ConfigUtils import ConfigUtils
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils


@gendocstring
class TranContentConfig(BaseModel, AmpelModelExtension):
	""" 
	Example: 

	.. sourcecode:: python\n
		{
			"content": {
				"docs": ["TRANSIENT", "COMPOUND", "PHOTOPOINT", "UPPERLIMIT", "T2RECORD"],
				"t2SubSelection": ["SNCOSMO", "CATALOGMATCH"]
			}
		}
	"""


	docs: Dict
	t2SubSelection: Dict = None

	@validator('docs', always=True, pre=True)
	def check_non_empty(cls, v):
		""" """
		if not v:
			cls.print_and_raise(
				header="transients->content->docs config error",
				msg='Parameter "docs" cannot be empty'
			)
		return v


	@validator('docs', 't2SubSelection', pre=True)
	def to_dict(cls, v, values, **kwargs):
		""" """

		if type(v) is str:
			return {'anyOf': v}

		if type(v) is list:
			if not AmpelUtils.check_seq_inner_type(v, str):
				cls.print_and_raise(
					header="transients->content->%s config error" % kwargs['field'],
					msg='dict value must be list containing strings'
				)
			return {'anyOf': v}

		if type(v) is dict:
			if 'anyOf' not in v or 'allOf' in v or len(v) !=1:
				cls.print_and_raise(
					header="transients->content->%s config error" % kwargs['field'],
					msg='dict value can only contain the key "anyOf"'
				)

		return v


	@validator('docs', pre=True)
	def check_flag_exist(cls, v, values, **kwargs):
		""" """
		return v


	@validator('docs', whole=True)
	def check_valid_docs(cls, value, values, **kwargs):
		""" """
		ConfigUtils.check_flags_from_dict(value, AlDocType, **kwargs)
		return value


	@validator('t2SubSelection')
	def validate_subselection(cls, t2SubSelection, values, **kwargs):
		"""
		Check transients->content->t2SubSelection
		"""

		# Docs should never be None (checked by prio validators)
		docs = values.get("docs").get("anyOf")

		if t2SubSelection.get("anyOf") and "T2RECORD" not in docs:
			cls.print_and_raise(
				header="T3 transients->select->docs config error",
				msg="T2RECORD must be defined in transients->select->docs\n"+
				"when transients->content->t2SubSelection " +
				"(%s)\n filtering is requested." % t2SubSelection.get("anyOf")
			)

		return t2SubSelection

	# TODO: check transient flags here
