#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/legacy/BaseT3Process.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 09.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional
from pydantic import validator
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.model.legacy.TranModel import TranModel


class BaseT3Process(AmpelStrictModel):

	schedule: List[str]
	tier: int
	name: str
	distrib: Optional[str]
	source: Optional[str]
	transients: TranModel


	@validator('tier')
	def consider_only_tier3_processes(cls, v):
		if v != 3:
			raise ValueError("T3ProcessModel not applicable")
		return v


	@validator('schedule', pre=True, whole=True)
	def cast_to_list(cls, v):
		if isinstance(v, str):
			return [v]
		return v


	@validator('transients', pre=True)
	def check_forbidden_channel_definition(cls, d):

		if "select" not in d:
			raise ValueError("Transient selection missing. Offending dict: %s" % d)

		if "channel" in d['select']:
			raise ValueError("Channel selection not permitted here. Offending dict: %s" % d)

		return d
