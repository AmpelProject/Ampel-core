#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeConstraintData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List
from ampel.common.docstringutils import gendocstring
from ampel.model.time.TimeDeltaData import TimeDeltaData
from ampel.model.time.TimeLastRunData import TimeLastRunData
from ampel.model.time.TimeStringData import TimeStringData
from ampel.model.time.UnixTimeData import UnixTimeData
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class TimeConstraintData(AmpelBaseModel):
	"""
	example1:
	
	TimeConstraintData(
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

	TimeConstraintData(
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
		TimeDeltaData, 
		TimeLastRunData, 
		TimeStringData, 
		UnixTimeData
	] = None

	after: Union[
		TimeDeltaData, 
		TimeLastRunData, 
		TimeStringData, 
		UnixTimeData
	] = None
