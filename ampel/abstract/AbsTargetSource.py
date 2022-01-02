#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                ampel/core/abstract/AbsTargetSource.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                15.08.2018
# Last Modified Date:  15.09.2018
# Last Modified By:    jvs

from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod


class AbsTargetSource(AmpelABC, abstract=True):
	""" Provides target fields for follow-up searches """

	@abstractmethod
	async def get_targets(self):
		"""
		:yields: a tuple ((ra,dec), radius, (date_min, date_max), [channels])
		"""
