#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsT4ControlUnit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                03.04.2023
# Last Modified Date:  04.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Generator
from ampel.types import Traceless, OneOrMany, ChannelId
from ampel.base.AmpelABC import AmpelABC
from ampel.content.T4Document import T4Document
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler
from ampel.log.AmpelLogger import AmpelLogger


class AbsT4ControlUnit(ContextUnit, AmpelABC, abstract=True):

	logger: Traceless[AmpelLogger]
	event_hdlr: Traceless[EventHandler]
	channel: None | OneOrMany[ChannelId] = None
	
	@abstractmethod
	def do(self) -> Generator[T4Document, None, None]:
		...
