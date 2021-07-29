#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/supply/SimpleT2BasedSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.07.2021
# Last Modified Date: 15.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Generator, Dict, Any
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.log.utils import safe_query_dict


class SimpleT2BasedSupplier(AbsT3Supplier):

	query: Dict[str, Any]

	def supply(self) -> Generator[AmpelBuffer, None, None]:

		if self.logger.verbose > 1: # log query parameters
			self.logger.debug(
				None, extra={
					'col': 2,
					'query': safe_query_dict(self.query, dict_key=None)
				}
			)

		# Retrieve pymongo cursor
		col = self.context.db.get_collection("t2")

		d: Dict[int, AmpelBuffer] = {}
		for el in col.find(self.query):
			if el['stock'] in d:
				d[el['stock']]['t2'].append(el) # type: ignore
			else:
				d[el['stock']] = AmpelBuffer(
					id = el['stock'],
					t2 = [el]
				)

		for ab in d.values():
			yield ab
