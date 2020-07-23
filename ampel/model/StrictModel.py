#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/StrictModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 14.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, BaseConfig, Extra
from typing import Any, Dict
from ampel.config.AmpelConfig import AmpelConfig

class StrictModel(BaseModel):
	"""
	In []: class A(StrictModel):
		...:     a: int
		...:     b: Optional[str]
		...:     c: Optional[int] = 12

	In []: %timeit A(a=12)
	9.97 µs ± 66.9 ns per loop (mean ± std. dev. of 7 runs, 100000 loops each)

	In []: AmpelConfig._check_types=0

	In []: %timeit A(a=12)
	3.71 µs ± 36.9 ns per loop (mean ± std. dev. of 7 runs, 100000 loops each)
	"""

	class Config(BaseConfig):
		arbitrary_types_allowed = True
		allow_population_by_field_name = True
		validate_all = True
		extra = Extra.forbid


	def __init__(self, **kwargs) -> None:
		""" Raises validation errors if extra fields are present """

		if AmpelConfig._check_types:
			self.__config__.extra = Extra.forbid
			BaseModel.__init__(self, **kwargs)
			self.__config__.extra = Extra.allow
		else:
			m = BaseModel.construct(**kwargs)
			object.__setattr__(self, '__dict__', m.__dict__)
			object.__setattr__(self, '__fields_set__', m.__fields_set__)


	def dict(self, **kwargs) -> Dict[str, Any]:
		if 'exclude_defaults' not in kwargs:
			kwargs['exclude_defaults'] = True
		return super().dict(**kwargs)
