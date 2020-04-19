#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/LegacyChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 19.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import List, Dict, Any, Union
from ampel.types import DataUnitModels
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate


class LegacyChannelTemplate(AbsChannelTemplate, abstract=True):
	"""
	Abstract class whose purpose is to maintain compatibility with channel
	definitions created for ampel versions < 0.7.
	This class must be subclassed.
	Known subclass: ZTFLegacyChannelTemplate
	"""
	auto_complete: Union[bool, str]
	t0_filter: DataUnitModels
	t2_compute: List[DataUnitModels] = []
	t3_supervize: List[Dict[str, Any]] = []

	@validator('t3_supervize', 't2_compute', pre=True, whole=True)
	def cast_to_list_if_required(cls, v):
		if isinstance(v, dict):
			return [v]
		return v
