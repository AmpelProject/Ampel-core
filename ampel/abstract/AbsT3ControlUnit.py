#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT3ControlUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 08.12.2021
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Generator
from ampel.types import Traceless, OneOrMany, ChannelId
from ampel.view.T3Store import T3Store
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler
from ampel.content.T3Document import T3Document
from ampel.log.AmpelLogger import AmpelLogger


class AbsT3ControlUnit(AmpelABC, ContextUnit, abstract=True):
	"""
	Abstract class for control T3 units, which like AbsT3PlainUnit but
	unlike AbsT3ReviewUnit, receive just a T3Store instance via
	the method process()
	"""

	logger: Traceless[AmpelLogger]
	event_hdlr: Traceless[EventHandler]
	channel: Optional[OneOrMany[ChannelId]] = None


	@abstractmethod
	def process(self, t3s: T3Store) -> Optional[Generator[T3Document, None, None]]:
		"""
		The content of the t3 store is dependent on:
		- the configuration of the 'include' option of the underlying t3 process
		- previously run t3 units if the option 'propagate' is activated
		"""
