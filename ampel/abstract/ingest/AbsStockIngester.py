#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStockIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.03.2020
# Last Modified Date: 30.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, List, Tuple, Union
from ampel.type import StockId, ChannelId
from ampel.base import abstractmethod
from ampel.abstract.ingest.AbsIngester import AbsIngester


class AbsStockIngester(AbsIngester, abstract=True):


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
		jextra: Dict[str, Any]
	) -> None:
		...
