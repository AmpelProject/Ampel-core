#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/t3/LoaderDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 16.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field, BaseModel, validator
from typing import Dict, Any, Optional, Type, Literal, Union, NewType

from ampel.utils.ReadOnlyDict import ReadOnlyDict
from ampel.utils.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel

DataClass = NewType('DataClass', Any)

@gendocstring
class LoaderDirective(AmpelBaseModel):
	""" """
	col: Literal["stock", "t0", "t1", "t2", "logs"]
	model: Optional[Type[Union[ReadOnlyDict, DataClass, BaseModel]]] = None
	# loader: Optional[Type] = None
	queryComplement: Optional[Dict[str, Any]] = Field(None, alias="query_complement")


	@validator('model')
	# pylint: disable=no-self-argument,no-self-use
	def check_dataclass(cls, v):
		""" """
		if v is ReadOnlyDict or issubclass(v, BaseModel):
			return v
			
		if not hasattr(v, '__dataclass_fields__'):
			raise ValueError("Invalid value provided")	
			
		return v
