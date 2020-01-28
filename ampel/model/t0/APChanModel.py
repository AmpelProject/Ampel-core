#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/t0/APChanModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.10.2019
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.t0.T0FilterModel import T0FilterModel

class APChanModel(AmpelBaseModel):
	""" """
	name: Union[int, str]
	dist_name: Optional[str]
	auto_complete: Union[bool, str]
	t0_add: T0FilterModel
