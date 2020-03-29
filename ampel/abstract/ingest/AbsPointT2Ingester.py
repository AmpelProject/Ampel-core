#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsPointT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 24.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Dict, Optional, Union
from ampel.types import StockId, ChannelId
from ampel.abc import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester


class AbsPointT2Ingester(AbsT2Ingester, abstract=True):


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		directives: Dict[ChannelId, Optional[Union[bool, int]]]
	) -> None:
		...
