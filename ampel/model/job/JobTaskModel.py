#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/JobTaskModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.04.2023
# Last Modified Date:  05.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal

from ampel.base.AmpelBaseModel import AmpelBaseModel


class JobTaskModel(AmpelBaseModel):
	"""
	A note about templates definitions:
	1) str | list[str]: templates are applied prior to job run
	2) same applies for dict[Literal['pre'], str | list[str]] -> 1) is just a shorthand for 2)
	3) dict[Literal['live'], str | list[str]] processes templates during job run,
	  just before the task is executed. It is required when templates
	  necessitate run-time information generated by previous tasks.
	The same template can be used as a 'pre' or 'live' template depending on the situation.
	For example, hash_t2_config can be used prior to job run for simple jobs.
	However, if run-time aliases are used, hash_t2_config should be requested as a 'live' template
	(just after resolve_run_time_aliases) as the hashing of unit configurations
	must occur after the inclusion of run-time information.
	"""

	# allow arbitrary content for templates
	model_config = {
		"extra": "allow"
	}

	title: None | str
	template: None | str | list[str] | dict[Literal['pre', 'live'], None | str | list[str]]
	unit: None | str # allow none as unit shall be set by template
	config: None | int | str | dict[str, Any]
	multiplier: int = 1

	def get_multiplier(self) -> int:
		return self.multiplier
