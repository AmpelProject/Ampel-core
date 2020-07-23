#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsDataPointIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Sequence, Generic, Any
from ampel.base import abstractmethod
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsIngester import AbsIngester
from ampel.type import StockId, TypeVar

T = TypeVar("T", bound=DataPoint)


class AbsDataPointIngester(Generic[T], AbsIngester, abstract=True):
	""" No known implementing class as of May 2020 """


	@abstractmethod
	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[Dict[str, Any]]
	) -> Sequence[T]:
		...
