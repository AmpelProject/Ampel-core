#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeLastRunModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import constr
from typing import Union, Dict
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel

@gendocstring
class TimeLastRunModel(AmpelBaseModel):
	use: constr(regex='.timeLastRun$')
	event: str
	fallback: Union[None, Dict] = None
