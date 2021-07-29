#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT3Supplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.07.2021
# Last Modified Date: 15.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Generator
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler


class AbsT3Supplier(AmpelABC, ContextUnit, abstract=True):
	"""
	Abstract class for T3 suppliers
	"""

	#: raise exceptions instead of catching and logging
	raise_exc: bool = True

	#: name of the associated process
	process_name: str


	def __init__(self,
		logger: AmpelLogger,
		event_hdlr: Optional[EventHandler] = None,
		**kwargs
	) -> None:

		super().__init__(**kwargs)

		# Non-serializable / not part of model / not validated; arguments
		self.logger = logger
		self.event_hdlr = event_hdlr


	@abstractmethod
	def supply(self) -> Generator[AmpelBuffer, None, None]:
		...
