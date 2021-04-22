#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/AbsT3Stager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2020
# Last Modified Date: 18.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List, Dict, Any, Optional, Generator

from ampel.type import ChannelId, Tag
from ampel.base import abstractmethod
from ampel.core.StockJournalUpdater import StockJournalUpdater
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Record import T3Record
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.AdminUnit import AdminUnit


class AbsT3Stager(AdminUnit, abstract=True):
	"""
	Supply stock views to one or more T3 units.
	"""

	logger: AmpelLogger
	jupdater: StockJournalUpdater

	channel: Optional[ChannelId] = None

	#: raise exceptions instead of catching and logging
	raise_exc: bool = True

	#: contextual information for this run
	session_info: Optional[Dict[str, Any]] = None

	#: number of buffers to process at once. Set to 0 to disable chunking
	chunk_size: int = 1000

	#: Cast ampel buffers into views for each t3 unit (that is possibly redundantly)
	#: since there is no real read-only struct in python
	paranoia: bool = True


	@abstractmethod
	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Optional[Union[T3Record, List[T3Record]]]:
		""" Process a chunk of AmpelBuffer instances """

	@abstractmethod
	def get_tags(self) -> Optional[List[Tag]]:
		""" Return collected T3Document tags """

	@abstractmethod
	def get_codes(self) -> Union[int, List[int]]:
		""" Return T3Document code(s) """
