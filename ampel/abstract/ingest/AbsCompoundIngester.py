#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsCompoundIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 19.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Generic, Iterable
from ampel.abc import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsIngester import AbsIngester
from ampel.types import StockId, TypeVar, ChannelId
from ampel.ingest.CompoundBluePrint import CompoundBluePrint

T = TypeVar("T", bound=CompoundBluePrint)


class AbsCompoundIngester(Generic[T], AbsIngester, abstract=True):


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		channels: Iterable[ChannelId]
	) -> T:
		...


	@abstractmethod
	def add_channel(self, channel: ChannelId):
		...
