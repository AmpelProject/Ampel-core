#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/AbsT3UnitRunner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2020
# Last Modified Date: 21.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Union, Dict, Any, Optional

from ampel.type import ChannelId
from ampel.base import abstractmethod
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.AdminUnit import AdminUnit


class AbsT3UnitRunner(AdminUnit, abstract=True):
	"""
	Supply stock views to one or more T3 units.
	"""

	logger: AmpelLogger
	run_id: int
	process_name: str
	channel: Optional[ChannelId] = None

	#: raise exceptions instead of catching and logging
	raise_exc: bool = True
	#: Record the invocation of this event in the journal of each selected stock
	update_journal: bool = True
	#: tag to add to journal records
	extra_journal_tag: Optional[Union[int, str]] = None
	#: contextual information for this run
	run_context: Optional[Dict[str, Any]] = None


	@abstractmethod
	def run(self, data: Sequence[AmpelBuffer]) -> None:
		"""Process a chunk of stocks"""
		...

	@abstractmethod
	def done(self) -> None:
		"""Signal that the run has finished"""
		...
