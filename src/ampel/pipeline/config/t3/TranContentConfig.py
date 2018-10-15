#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/TranContentConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 11.10.2018
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


	docs: List[AlDocType]
	t2SubSelection: List[str] = None


	@validator('docs', whole=True, pre=True, always=True)
	def convert_to_enum(cls, v, values, **kwargs):
		""" """

		if not v:
			cls.print_and_raise(
				header="transients->content->docs config error",
				msg='Parameter "docs" cannot be empty'
			)

		# Due to pydantic bug, validators can be called twice
		if AmpelUtils.check_seq_inner_type(v, (int, AlDocType)):
			return v
		else:
			# For convenience and syntax consistency, we accept dicts
			if isinstance(v, dict):
				return cls.logic_dict_to_list("docs", v)

			if not isinstance(v, str) and not AmpelUtils.check_seq_inner_type(v, str):
				cls.print_and_raise(
					header="transients->content->docs config error",
					msg='List values must be string'
				)

		ret = []

		for el in AmpelUtils.iter(v):

			if type(el) is str:
				try:
					ret.append(AlDocType[el])
				except KeyError:
					cls.print_and_raise(
						header="transients->select->docs config error",
						msg="Unknown flag '%s'.\nPlease check class AlDocType for allowed flags" % el
					)
			else:
				raise ValueError("Unexpected format")

		return ret


	@validator('t2SubSelection', pre=True, whole=True)
	def to_seq(cls, v, values, **kwargs):
		""" """

		if AmpelUtils.is_sequence(v):
		
			# Due to pydantic bug, validators can be called twice
			if AmpelUtils.check_seq_inner_type(v, str):
				return v
			else:
				# For convenience and syntax consistency, we accept dicts
				if isinstance(v, dict):
					return cls.logic_dict_to_list("t2SubSelection", v)

				cls.print_and_raise(
					header="transients->content->t2SubSelection config error",
					msg='List values must be string'
				)

		if type(v) is str:
			return [v]


		cls.print_and_raise(
			header="transients->content->t2SubSelection unknown format",
			msg='Offending value: %s' % v
		)


	@classmethod
	def logic_dict_to_list(cls, field, v):
		"""
		For convenience and syntax consistency, we accept docs format such as: 
		{'anyOf': ['a', 'b]} which we convert as simple list
		"""
		if 'anyOf' not in v or 'allOf' in v or len(v) !=1:
			cls.print_and_raise(
				header="transients->content->%s config error" % field,
				msg='Dict value can only contain the key "anyOf"'
			)

		return v['anyOf']


	@validator('t2SubSelection')
	def validate_subselection(cls, t2SubSelection, values, **kwargs):
		"""
		Check transients->content->t2SubSelection
		"""

		# Docs should never be None (checked by prior validators)
		docs = values.get("docs")

		if AlDocType.T2RECORD not in docs:
			cls.print_and_raise(
				header="T3 transients->select->docs config error",
				msg="T2RECORD must be defined in transients->select->docs\n"+
				"when transients->content->t2SubSelection filtering is requested."
			)

		return t2SubSelection
