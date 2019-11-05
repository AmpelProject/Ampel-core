#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/ChannelData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List, Optional
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class ChannelData(AmpelBaseModel):
	""" 
	"""
	channel: Union[int, str]
	hash: Optional[int]
	distName: str = ""
	active: bool = True
	contact: str
	access: Optional[List[str]]
	policy: List[str] = []
