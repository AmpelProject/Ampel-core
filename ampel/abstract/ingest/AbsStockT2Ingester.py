#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStockT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 30.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Tuple, Union
from ampel.type import StockId, ChannelId
from ampel.base import abstractmethod
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester


class AbsStockT2Ingester(AbsT2Ingester, abstract=True):

	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> None:
		...
