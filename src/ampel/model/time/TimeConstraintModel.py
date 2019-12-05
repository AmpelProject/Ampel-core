#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeConstraintModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List
from ampel.common.docstringutils import gendocstring
from ampel.model.time.TimeDeltaModel import TimeDeltaModel
from ampel.model.time.TimeLastRunModel import TimeLastRunModel
from ampel.model.time.TimeStringModel import TimeStringModel
from ampel.model.time.UnixTimeModel import UnixTimeModel
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class TimeConstraintModel(AmpelBaseModel):
	"""
	example1:
	
	TimeConstraintModel(
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

	TimeConstraintModel(
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
		TimeDeltaModel, 
		TimeLastRunModel, 
		TimeStringModel, 
		UnixTimeModel
	] = None

	after: Union[
		TimeDeltaModel, 
		TimeLastRunModel, 
		TimeStringModel, 
		UnixTimeModel
	] = None
