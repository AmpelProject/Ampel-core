#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/T3IncludeDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.12.2021
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Optional, Union
from ampel.types import OneOrMany
from ampel.model.UnitModel import UnitModel


class T3IncludeDirective(BaseModel):
	"""
	:param session: models for AbsT3Supplier[dict] instances which populates the 'session' field of T3Store
	Examples of session information are:
	- Date and time the current process was last run
	- Number of alerts processed since then
	"""

	#: Provides Iterable[T3Document]
	docs: Optional[UnitModel]

	#: Provides session information. Unit(s) must be a subclass of AbsT3Supplier
	session: Union[None, OneOrMany[UnitModel]]
