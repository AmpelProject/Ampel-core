#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/TranConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.GettableConfig import GettableConfig
from ampel.pipeline.config.t3.TranSelectConfig import TranSelectConfig
from ampel.pipeline.config.t3.TranContentConfig import TranContentConfig


@gendocstring
class TranConfig(BaseModel, GettableConfig):
	""" 
	Example: 
	{
		"transients": {
	    	"select": {
	    		"created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
	    		"modified": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
	    		"channels": "HU_GP_CLEAN",
				"withFlags": "INST_ZTF",
	    		"withoutFlags": "HAS_ERROR"
			},
			"state": "$latest",
			"content": {
				"docs": ["TRANSIENT", "COMPOUND", "PHOTOPOINT", "UPPERLIMIT", "T2RECORD"],
				"t2SubSelection": ["SNCOSMO", "CATALOGMATCH"]
			}
		}
	}
	"""
	state: str
	select: TranSelectConfig = None
	content: TranContentConfig = None
	verbose: bool = True
	debug: bool = False
	chunk: int = 200


	@validator('state')
	def validate_state(cls, state):
		"""
		"""
		if state != "$latest" and state != "$all":
			raise ValueError('Parameter "state" must be either "$latest" of "$all"')

		return state
