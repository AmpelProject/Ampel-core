#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/TranSelectConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 22.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Union, List
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.time.TimeConstraintConfig import TimeConstraintConfig
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.config.ConfigUtils import ConfigUtils
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.base.flags.TransientFlags import TransientFlags
from ampel.pipeline.config.t3.AllOf import AllOf
from ampel.pipeline.config.t3.AnyOf import AnyOf


@gendocstring
class TranSelectConfig(BaseModel, AmpelModelExtension):
	"""
	Example: 
	.. sourcecode:: python\n
		{
			"select": {
				"created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
				"modified": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
				"channels": "HU_GP_CLEAN",
				"withFlags": "INST_ZTF",
				"withoutFlags": "HAS_ERROR"
			}
		}
	"""

	created: Union[None, TimeConstraintConfig] = None
	modified: Union[None, TimeConstraintConfig] = None
	channels: Union[None, AnyOf, AllOf] = None
	withFlags: Union[None, AnyOf, AllOf] = None
	withoutFlags: Union[None, AnyOf, AllOf] = None


	@validator('channels', 'withFlags', 'withoutFlags', pre=True, whole=True)
	def cast(cls, v, values, **kwargs):

		field_name = kwargs['field'].name.split("_")[0]

		if type(v) is list:
			cls.print_and_raise(
				header="transients->select->%s config error" % field_name,
				msg= \
					"'%s' parameter cannot be a list because it would be\n" % field_name + 
					"ambiguous to interpret. Please rather use the following syntax:\n" +
					" -> {'any': ['Ab', 'Cd', 'Ef']}\n" + 
					"or\n" + 
					" -> {'all': ['Ab', 'Cd', 'Ef']}\n\n" + 
					"One level nesting is allowed, please the see\n" + 
					"ConfigUtils.conditional_expr_converter(..) docstring for more info"
			)

		if type(v) in (str, int):
			return {'anyOf': [v]}

		if type(v) is dict:

			if len(v) != 1:
				cls.print_and_raise(
					header="transients->select->%s config error" % field_name,
					msg="Unsupported dict format %s" % v
				)

			if 'anyOf' in v:

				if not AmpelUtils.is_sequence(v['anyOf']):
					cls.print_and_raise(
						header="transients->select->%s:anyOf config error" % field_name,
						msg="Invalid dict value type: %s. Must be a sequence" % type(v['anyOf'])
					)

				# 'anyOf' supports only a list of dicts and str/int
				if not AmpelUtils.check_seq_inner_type(v['anyOf'], (str, int, dict), multi_type=True):
					cls.print_and_raise(
						header="transients->select->%s:anyOf config error" % field_name,
						msg="Unsupported nesting (err 2)"
					)

				if not AmpelUtils.check_seq_inner_type(v['anyOf'], (int, str)) and len(v['anyOf']) < 2:
					cls.print_and_raise(
						header="transients->select->%s:anyOf config error" % field_name,
						msg="anyOf list must contain more than one element when containing allOf\n" + 
						"Offending value: %s" % v
					)
			
				for el in v['anyOf']:

					if isinstance(el, dict):

						if 'anyOf' in el:
							cls.print_and_raise(
								header="transients->select->%s:anyOf.anyOf config error" % field_name,
								msg="Unsupported nesting (anyOf in anyOf)"
							)

						elif 'allOf' in el:

							# 'allOf' closes nesting  
							if not AmpelUtils.check_seq_inner_type(el['allOf'], (int, str)):
								cls.print_and_raise(
									header="transients->select->%s:anyOf.allOf config error" % field_name,
									msg="Unsupported nesting (allOf list content must be str/int)"
								)

							if len(set(el['allOf'])) < 2:
								cls.print_and_raise(
									header="transients->select->%s:allOf config error" % field_name,
									msg="Please do not use allOf with just one element\n" + 
									"Offending value: %s" % el
							)

						else:
							cls.print_and_raise(
								header="transients->select->%s:anyOf config error" % field_name,
								msg="Unsupported nested dict: %s" % el
							)

			elif 'allOf' in v:

				# 'allOf' closes nesting  
				if not AmpelUtils.is_sequence(v['allOf']) or not AmpelUtils.check_seq_inner_type(v['allOf'], (int, str)):
					cls.print_and_raise(
						header="transients->select->%s:allOf config error" % field_name,
						msg="Invalid type for value %s\n(must be a sequence, is: %s)\n" % 
							(v['allOf'], type(v['allOf'])) + 
							"Note: no nesting is allowed below 'allOf'"
					)

				if len(set(v['allOf'])) < 2:
					cls.print_and_raise(
						header="transients->select->%s:allOf config error" % field_name,
						msg="Please do not use allOf with just one element\n" + 
						"Offending value: %s" % v
					)

			else: 
				cls.print_and_raise(
					header="transients->select->%s config error" % field_name,
					msg="Invalid dict key (only 'anyOf' and 'allOf' allowed)"
				)

		return v


	@validator('withFlags', 'withoutFlags', whole=True)
	def check_valid_flag(cls, value, values, **kwargs):
		""" """
		ConfigUtils.check_flags_from_dict(value, TransientFlags, **kwargs)
		return value
