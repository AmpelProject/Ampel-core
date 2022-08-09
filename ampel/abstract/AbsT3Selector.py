#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsT3Selector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.12.2019
# Last Modified Date:  17.02.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import ClassVar
from collections.abc import Iterable
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit


class AbsT3Selector(AmpelABC, ContextUnit, abstract=True):

	#: field used to identify stocks
	field_name: ClassVar[str] = "stock"

	@abstractmethod
	def fetch(self) -> None | Iterable:
		""" Get selected stock ids """
