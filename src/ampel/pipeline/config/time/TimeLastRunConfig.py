#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/time/TimeLastRunConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 16.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, constr
from typing import Union, Dict
from ampel.pipeline.common.docstringutils import gendocstring

from ampel.pipeline.config.time.TimeDeltaConfig import TimeDeltaConfig
from ampel.pipeline.config.time.TimeStringConfig import TimeStringConfig
from ampel.pipeline.config.time.UnixTimeConfig import UnixTimeConfig

@gendocstring
class TimeLastRunConfig(BaseModel):
	use: constr(regex='.timeLastRun$')
	event: str
	fallback: Union[
	    TimeDeltaConfig,
	    TimeStringConfig,
	    UnixTimeConfig
	] = TimeDeltaConfig(use='$timeDelta', arguments={'days': -1})
