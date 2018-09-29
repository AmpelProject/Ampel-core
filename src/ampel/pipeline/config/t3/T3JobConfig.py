#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/T3JobConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Union, List
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.T3JobTranConfig import T3JobTranConfig
from ampel.pipeline.config.channel.T3TaskConfig import T3TaskConfig

@gendocstring
class T3JobConfig(BaseModel):
	"""
	Possible 'schedule' values (https://schedule.readthedocs.io/en/stable/):
	"every(10).minutes"
	"every().hour"
	"every().day.at("10:30")"
	"every().monday"
	"every().wednesday.at("13:15")"
	"""
	name: str
	active: bool = True
	globalInfo: bool = False
	schedule: Union[str, List[str]]
	transients: Union[None, T3JobTranConfig]
	tasks: Union[T3TaskConfig, List[T3TaskConfig]]
