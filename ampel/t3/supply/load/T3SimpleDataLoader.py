#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/supply/load/T3SimpleDataLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 29.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.codec_options import CodecOptions
from typing import Iterable, Union, Iterator, Optional
from ampel.types import StockId, StrictIterable
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.abstract.AbsT3Loader import AbsT3Loader
from ampel.mongo.view.FrozenValuesDict import FrozenValuesDict


class T3SimpleDataLoader(AbsT3Loader):
	"""Load all requested documents for the selected stocks"""

	codec_options: Optional[CodecOptions] = CodecOptions(document_class=FrozenValuesDict)

	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]]
	) -> Iterable[AmpelBuffer]:

		return self.data_loader.load(
			stock_ids = stock_ids,
			directives = self.directives,
			channel = self.channel,
			codec_options = self.codec_options,
			logger = self.logger
		)
