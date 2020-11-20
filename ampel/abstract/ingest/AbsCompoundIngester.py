#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsCompoundIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 08.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import WARNING
from typing import Sequence, Generic, List, Tuple, Union, Optional
from ampel.base import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsIngester import AbsIngester
from ampel.type import StockId, TypeVar, ChannelId
from ampel.ingest.CompoundBluePrint import CompoundBluePrint
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler

T = TypeVar("T", bound=CompoundBluePrint)


class AbsCompoundIngester(Generic[T], AbsIngester, abstract=True):


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> Optional[T]:
		...


	@abstractmethod
	def add_channel(self, channel: ChannelId) -> None:
		...


	def log_records_to_logd(self,
		rec_buffer: RecordBufferingHandler, clear: bool = True
	) -> None:
		"""
		Compound ingesters can make use of a T1 unit underneath,
		which requires a logger like every other base units.
		The logger used in usually associated with a buffering handler
		whose logs are later transfered to the LogsBufferDict instance
		used and shared among ingesters.
		"""

		logd = self.logd
		for rec in rec_buffer.buffer:
			if rec.msg:
				logd['logs'].append(rec.msg) # type: ignore
			if rec.levelno > WARNING:
				logd['err'] = True
			if hasattr(rec, 'extra'):
				for k, v in rec.extra: # type: ignore
					logd['extra'][k] = v

		if clear:
			rec_buffer.buffer = []
