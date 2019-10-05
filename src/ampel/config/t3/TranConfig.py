#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/t3/TranConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from ampel.common.docstringutils import gendocstring
from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.AmpelBaseModel import AmpelBaseModel
from ampel.config.t3.TranSelectConfig import TranSelectConfig
from ampel.config.t3.TranContentConfig import TranContentConfig


@gendocstring
class TranConfig(AmpelBaseModel):
	""" 
	Example: 
	{
		"transients": {
	    	"select": {
	    		"created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
	    		"modified": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
	    		"channels": "HU_GP_CLEAN",
				"withTags": "SURVEY_ZTF",
	    		"withoutTags": "HAS_ERROR"
			},
			"state": "$latest",
			"content": {
				"docs": [
					"TRANSIENT",
					"COMPOUND",
					"DATAPOINT",
					"T2RECORD",
					{
						col: "t2",
						query: {
							'unitId': 'T2SNCOSMO',
							'docId': '#latestDefault'
						}
					},
					"#LATEST_T2RECORD"
				],
				"t2SubSelection": ["SNCOSMO", "CATALOGMATCH"]
			}
		}

		whereby "#latestDefault" is defined in the AmpelConfig aliases as:
		'alias': {
			't3': {
				'docs': {
					'#latestDefault': {
						'unitId': 'QueryLatestCompound'
					},
					"#LATEST_T2RECORD": {
						col: "t2",
						query: {
							'docId': '#latestDefault'
						}
					}
				}
			}
		}
	}
	"""
	state: str
	select: TranSelectConfig = None
	content: TranContentConfig = None
	verbose: bool = False
	debug: bool = False
	chunk: int = 200


	@validator('state', always=True, pre=True)
	def validate_state(cls, state):
		"""
		"""
		if state != "$latest" and state != "$all":
			raise ValueError(
				"Parameter 'state' must be either '$latest' of '$all'" +
				" (Offending value: %s)" % state
			)

		return state
