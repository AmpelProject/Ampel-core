#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/QueryTimeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.12.2019
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import Union, Optional, Dict, Any
from ampel.util.docstringutils import gendocstring
from ampel.model.AmpelStrictModel import AmpelStrictModel


@gendocstring
class QueryTimeModel(AmpelStrictModel):
	"""
	Standardized parameter for the class QueryMatchStock
	"""
	before: Optional[Union[int, float]] = Field(None, alias='$lt')
	after: Optional[Union[int, float]] = Field(None, alias='$gt')

	# pylint: disable=arguments-differ
	def dict(self, **kwargs) -> Dict[str, Any]:
		"""
		Example:
		{
			'$gt': 1575000000.003819,
			'$lt': 1575966106.003819
		}
		"""
		return super().dict(
			**{**kwargs, "by_alias": True, "exclude_none": True} # type: ignore
		)
