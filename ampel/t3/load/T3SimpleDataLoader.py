#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/load/T3SimpleDataLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 16.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable, Union, Iterator
from ampel.type import StockId, StrictIterable
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.t3.load.AbsT3Loader import AbsT3Loader


class T3SimpleDataLoader(AbsT3Loader):
	"""Load all requested documents for the selected stocks"""

	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]]
	) -> Iterable[AmpelBuffer]:

		return self.db_content_loader.load(
			stock_ids = stock_ids,
			directives = self.directives,
			channel = self.channel
		)
