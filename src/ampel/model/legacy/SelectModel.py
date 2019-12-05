#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/legacy/SelectModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union
from pydantic import validator
from ampel.common.docstringutils import gendocstring
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.time.TimeConstraintModel import TimeConstraintModel
from ampel.config.utils.LogicSchemaUtils import LogicSchemaUtils


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

	created: Union[None, TimeConstraintModel] = None
	modified: Union[None, TimeConstraintModel] = None
	withTags: Union[None, AnyOf, AllOf, OneOf] = None
	withoutTags: Union[None, AnyOf, AllOf, OneOf] = None
	channels: Union[None, AnyOf, AllOf, OneOf] = None


	@validator('channels', 'withTags', 'withoutTags', pre=True, whole=True)
	def cast(cls, v, values, **kwargs):
		""" """
		return LogicSchemaUtils.to_logical_struct(
			v, kwargs['field'].name.split("_")[0]
		)
