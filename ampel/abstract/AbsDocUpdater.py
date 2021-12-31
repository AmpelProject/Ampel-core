#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsDocUpdater.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                28.05.2021
# Last Modified Date:  10.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel


class AbsDocUpdater(AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Aim at facilating updates of documents regardless of the underlying database system.
	Not in use yet
	"""

	@abstractmethod
	def update(self,
		tier: Literal[0, 1, 2],
		match: dict[str, Any],
		let: dict[str, Any],
		push: dict[str, Any],
		add_to_set: dict[str, Any]
	) -> None:
		...
