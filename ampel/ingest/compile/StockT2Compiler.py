#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/compile/StockT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2020
# Last Modified Date: 01.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Set, List, Union, Tuple, Optional
from ampel.type import ChannelId
from ampel.abstract.ingest.AbsStockT2Compiler import AbsStockT2Compiler


class StockT2Compiler(AbsStockT2Compiler):
	"""
	Helper class capabable of generating a nested dict that is used as basis to create T2 documents.
	The generated structure is optimized: multiple T2 documents associated with the same stock record
	accross different channels are merged into one single T2 document
	that references all corrsponding channels.
	"""

	def compile(self,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> Dict[Tuple[str, Optional[int]], Set[ChannelId]]:
		"""
		TLDR: This function computes and returns a dict structure helpful for creating T2 docs.
		This computation is required since:
		* A given alert can be accepted by one filter and be rejected by the other
		* T0 filters can return a customized set of T2 units to be run (group id)
		:param chan_selection: example: {"CHAN_SN": True, "CHAN_GRB": 1, "CHAN_BH": None, "CHAN_A": -2}

		:returns: Dict[(unit, config): set(<channel ids>)]
		"""

		t2s_eff: Dict[Tuple[str, Optional[int]], Set[ChannelId]] = {}

		for chan, ingest_model in self.get_ingest_models(chan_selection):

			k = (ingest_model.unit_id, ingest_model.config)
			if k not in t2s_eff:
				t2s_eff[k] = {chan}
			else:
				t2s_eff[k].add(chan)

		return t2s_eff
