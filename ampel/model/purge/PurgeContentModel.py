#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/purge/PurgeContentModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.06.2020
# Last Modified Date:  18.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from datetime import timedelta
from typing import Literal

from ampel.base.AmpelBaseModel import AmpelBaseModel


class TimeDeltaModel(AmpelBaseModel):

	days: int = 0
	seconds: int = 0
	microseconds: int = 0
	milliseconds: int = 0
	minutes: int = 0
	hours: int = 0
	weeks: int = 0

	def timedelta(self) -> timedelta:
		return timedelta(
			days=self.days, seconds=self.seconds, microseconds=self.microseconds,
			milliseconds=self.milliseconds, minutes=self.minutes,
			hours=self.hours, weeks=self.weeks
		)


class PurgeContentModel(AmpelBaseModel):
	"""
	:param delay: time of inactivity in days after which entities will be purged
	:param format: output format exported by ampel.
	- bson: (native format) use this format if you plan on working on archives with your own mongodb and ampel tools
	- json: ampel ids and tags will be converted from hashes back to strings
	:param unify: whether to merge documents from all tiers into one unique document for each stock id
	This can facilitate further analysis. Note that because BSON document cannot exceed 16MB,
	this parameter can only by used in combination with the 'json' format.
	:param compress: whether to compress the output file

	Note: For now, we do not offer the option to include logs into the 'merged' structure.
	The purge of logs and content is done with two different, independant processes.
	Feel free to implement your own UnifiedPurger and associated UnifiedPurgeModel if you need to.
	"""
	delay: TimeDeltaModel = TimeDeltaModel()
	format: Literal['bson', 'json']
	unify: bool = False
	compress: bool = True

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		if self.format == 'bson' and self.unify:
			raise ValueError(
				"Parameter 'unify' can only by used in combination with " +
				"the 'json' format (please see docstring)"
			)
