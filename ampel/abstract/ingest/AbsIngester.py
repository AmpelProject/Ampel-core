#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 21.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Any
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LogsBufferDict import LogsBufferDict
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit


class AbsIngester(AbsProcessorUnit, abstract=True):

	logger: AmpelLogger
	logd: LogsBufferDict
	updates_buffer: DBUpdatesBuffer
	run_id: int
	hash: Optional[int]

	def get_stats(self, reset: bool = True) -> Optional[Dict[str, Any]]:
		return None

	def __hash__(self) -> int:
		return self.hash

	def __eq__(self, other) -> bool:
		if isinstance(other, int):
			return self.hash == other
		elif isinstance(other, AbsIngester):
			return self.hash == other.hash
		else:
			return False
