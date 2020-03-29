#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStockIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.03.2020
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Iterable, Any
from ampel.types import StockId, ChannelId
from ampel.abc import abstractmethod
from ampel.abstract.ingest.AbsIngester import AbsIngester


class AbsStockIngester(AbsIngester, abstract=True):


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		chan_names: Iterable[ChannelId],
		jextra: Dict[str, Any]
	) -> None:
		...
