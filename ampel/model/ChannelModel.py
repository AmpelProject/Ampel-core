#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ChannelModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 09.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List, Optional
from ampel.model.AmpelStrictModel import AmpelStrictModel


class ChannelModel(AmpelStrictModel):

	channel: Union[int, str]
	active: bool = True
	hash: Optional[int]
	distrib: Optional[str]
	source: Optional[str]
	contact: Optional[str]
	access: Optional[List[str]]
	policy: List[str] = []
