#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsT3Supplier.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                15.07.2021
# Last Modified Date:  13.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Generic
from ampel.types import Traceless, T
from ampel.view.T3Store import T3Store
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler


class AbsT3Supplier(Generic[T], AmpelABC, ContextUnit, abstract=True):
	"""
	Abstract class for T3 suppliers
	"""

	logger: Traceless[AmpelLogger]
	event_hdlr: Traceless[EventHandler]

	@abstractmethod
	def supply(self, t3s: T3Store) -> T:
		...
