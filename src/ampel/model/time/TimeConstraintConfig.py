#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeConstraintConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List
from ampel.common.docstringutils import gendocstring
from ampel.model.time.TimeDeltaConfig import TimeDeltaConfig
from ampel.model.time.TimeLastRunConfig import TimeLastRunConfig
from ampel.model.time.TimeStringConfig import TimeStringConfig
from ampel.model.time.UnixTimeConfig import UnixTimeConfig
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class TimeConstraintConfig(AmpelBaseModel):
	"""
	example1:
	
	TimeConstraintConfig(
	   **{
			"after": {
				"use": "$timeDelta",
				"arguments" :  {
				   	"days" : -40
			   	}
			},
			"before": {
				"use": "timeString",
				"dateTimeStr": "21/11/06 16:30",
				"dateTimeFormat": "%d/%m/%y %H:%M"
			}
		}
	)

	example2: 

	TimeConstraintConfig(
	   **{
			"after": {
				"use": "$timeLastRun",
				"jobName": "val_test"
			},
			"before": {
				"use": "unixTime",
				"value": 1531306299
			}
		}
	)
	"""

	before: Union[
		TimeDeltaConfig, 
		TimeLastRunConfig, 
		TimeStringConfig, 
		UnixTimeConfig
	] = None

	after: Union[
		TimeDeltaConfig, 
		TimeLastRunConfig, 
		TimeStringConfig, 
		UnixTimeConfig
	] = None
