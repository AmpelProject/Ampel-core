#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/time/QueryTimeModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.12.2019
# Last Modified Date:  06.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.base.AmpelBaseModel import AmpelBaseModel


class QueryTimeModel(AmpelBaseModel):
	""" Standardized parameter for class QueryMatchStock """

	before: None | int | float = None
	after: None | int | float = None

	def __bool__(self) -> bool:
		return self.before is not None or self.after is not None

	def dict(self, **kwargs) -> dict[str, Any]:
		d = super().dict(**kwargs)
		d['$lt'] = d.pop('before')
		d['$gt'] = d.pop('after')
		return {k: v for k, v in d.items() if v is not None}
