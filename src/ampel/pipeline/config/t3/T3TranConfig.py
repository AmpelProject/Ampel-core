#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TranConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.t3.T3TranSelectConfig import T3TranSelectConfig
from ampel.pipeline.config.t3.T3TranLoadConfig import T3TranLoadConfig


class MyValueError(ValueError):
	pass

@gendocstring
class T3TranConfig(BaseModel):
	""" """
	select: T3TranSelectConfig = None
	load: T3TranLoadConfig = None
	chunk: int = 200


	@validator('load')
	def check_correct_use_of_t2s(cls, load):
		"""
		Check transients->select->t2s
		"""

		if load.docs and load.t2s:
			if (
				(AmpelUtils.is_sequence(load.docs) and "T2RECORD" not in load.docs) or
				(type(load.docs) is str and "T2RECORD" != load.docs)
			):
				raise MyValueError(
					"T3 config error: T2RECORD must be defined in transients->select->docs "+
					"when transients->load->t2s (%s) filtering is requested." % load.t2s
				)

		return load

