#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStateT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Union
from ampel.types import StockId, ChannelId
from ampel.abc import abstractmethod
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester
from ampel.ingest.CompoundBluePrint import CompoundBluePrint


class AbsStateT2Ingester(AbsT2Ingester, abstract=True):


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		comp_blueprint: CompoundBluePrint,
		filter_res: Dict[ChannelId, Optional[Union[bool, int]]]
	) -> None:
		...
