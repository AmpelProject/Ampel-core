#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/template/LegacyChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Union, List #, Dict, Any
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate


class LegacyChannelTemplate(AbsChannelTemplate, abstract=True):
	""" 
	Abstract class whose purpose is to maintain compatibility with channel 
	definitions created for ampel versions < 0.7.
	This class must be subclassed.
	Known subclass: ZTFLegacyChannelTemplate
	"""
	autoComplete: Union[bool, str]
	t0Filter: UnitModel
	t2Compute: List[UnitModel] = []
	# bugging with pydantic 1.0, no idea why..
	#t3Supervize: List[Dict[str, Any]] = []
	t3Supervize: List = []


	@validator('t3Supervize', 't2Compute', pre=True, whole=True)
	def cast_to_list_if_required(cls, v):
		if isinstance(v, dict):
			return [v]
		return v
