#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelBuffer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.05.2018
# Last Modified Date: 14.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Dict, List, Any, TypedDict, Literal
from ampel.type import StockId

# Please update BufferKey on AmpelBuffer udpates
# There is currently unfortunately no way of extracting a Literal out of a TypedDict
BufferKey = Literal['id', 'stock', 't0', 't1', 't2', 'logs', 'extra']

class AmpelBuffer(TypedDict, total=False):

	# could stock be of type List[Dict[str, Any]] for hybrid/dual transients ?
	id: StockId
	stock: Optional[Dict[str, Any]]
	t0: Optional[List[Dict[str, Any]]]
	t1: Optional[List[Dict[str, Any]]]
	t2: Optional[List[Dict[str, Any]]]
	logs: Optional[List[Dict[str, Any]]]
	extra: Optional[Dict[str, Any]]
