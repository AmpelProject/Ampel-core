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
	:param update_journal: Record the invocation of this event
	in the journal of each selected transient
	"""

	logger: AmpelLogger
	run_id: int
	process_name: str
	channel: Optional[ChannelId] = None

	raise_exc: bool = False
	update_journal: bool = True
	extra_journal_tag: Optional[Union[int, str]] = None
	run_context: Optional[Dict[str, Any]] = None


	@abstractmethod
	def run(self, data: Sequence[AmpelBuffer]) -> None:
		...
