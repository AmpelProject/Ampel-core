#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/QueryTimeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.12.2019
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import Union, Optional
from ampel.utils.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class QueryTimeModel(AmpelBaseModel):
	"""
	Standardized parameter for the class QueryMatchStock
	"""
	before: Optional[Union[int, float]] = Field(None, alias='$lt') 
	after: Optional[Union[int, float]] = Field(None, alias='$gt')

	# pylint: disable=arguments-differ
	def dict(self, **kwargs):
		"""
		Example:
		{
			'$gt': 1575000000.003819,
			'$lt': 1575966106.003819
		}
		"""
		return super().dict(
			**{**kwargs, "by_alias": True, "exclude_none": True}
		)
