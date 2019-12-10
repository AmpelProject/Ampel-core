#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/legacy/SelectModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Union, Optional
from ampel.common.docstringutils import gendocstring
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.time.TimeConstraintModel import TimeConstraintModel
from ampel.config.LogicSchemaUtils import LogicSchemaUtils


@gendocstring
class SelectModel(AmpelBaseModel):
	"""
	Example: 
	.. sourcecode:: python\n
	{
		"select": {
			"created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
			"modified": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
			"channels": "HU_GP_CLEAN",
			"withTags": "SURVEY_ZTF",
			"withoutTags": "HAS_ERROR"
		}
	}
	"""

	created: Optional[TimeConstraintModel] = None
	modified: Optional[TimeConstraintModel] = None
	channels: Optional[Union[AnyOf, AllOf, OneOf]] = None
	withTags: Optional[Union[AnyOf, AllOf, OneOf]] = None
	withoutTags: Optional[Union[AnyOf, AllOf, OneOf]] = None

	# pylint: disable=unused-argument,no-self-argument,no-self-use
	@validator('channels', 'withTags', 'withoutTags', pre=True, whole=True)
	def cast(cls, v, values, **kwargs):
		""" """
		return LogicSchemaUtils.to_logical_struct(
			v, kwargs['field'].name.split("_")[0]
		)
