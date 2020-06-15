#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/legacy/TranModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.util.docstringutils import gendocstring
from ampel.model.StrictModel import StrictModel
from ampel.model.legacy.SelectModel import SelectModel
from ampel.model.legacy.ContentModel import ContentModel

@gendocstring
class TranModel(StrictModel):
	""" 
	"""
	select: SelectModel = None
	content: ContentModel = None
	chunk: Optional[int]
