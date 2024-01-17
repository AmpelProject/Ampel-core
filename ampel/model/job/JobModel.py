#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/JobModel.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  05.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.ChannelModel import ChannelModel
from ampel.model.job.EnvSpec import EnvSpec
from ampel.model.job.JobTaskModel import JobTaskModel
from ampel.model.job.MongoOpts import MongoOpts


class JobModel(AmpelBaseModel):

	name: None | str
	sig: None | int
	requirements: list[str] = []
	env: dict[str, EnvSpec] = {}
	channel: list[ChannelModel] = []
	alias: dict[Literal["t0", "t1", "t2", "t3", "t4"], dict[str, Any]] = {}
	mongo: MongoOpts = MongoOpts()
	task: list[JobTaskModel]


	def __init__(self, **kwargs):

		# Allow 'name' channel name alias to avoid 'channel[].channel'
		for c in kwargs.get('channel', []):
			if isinstance(c, dict) and 'name' in c:
				c["channel"] = c.pop("name")

		if isinstance(kwargs.get('task'), dict):
			kwargs['task'] = [kwargs['task']]

		super().__init__(**kwargs)
