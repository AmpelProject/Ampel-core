#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/IngestConfigOptions.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.09.2021
# Last Modified Date: 12.09.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Tuple, Literal, Union, Optional
from ampel.model.StrictModel import StrictModel

class IngestConfigOptions(StrictModel):

	#: Aux unit name
	filter: Optional[str]

	#: Dict key
	sort: Optional[str]

	#: slice arguments or 'first' / 'last'
	select: Union[
		None,
		Literal['first'],
		Literal['last'],
		Tuple[Optional[int], Optional[int], Optional[int]]
	]

	def __init__(self, **kwargs):
		if 'select' in kwargs and isinstance(kwargs['select'], list) and len(kwargs['select']) == 3:
			kwargs['select'] = tuple(kwargs['select'])
		super().__init__(**kwargs)
		if self.sort and not self.select:
			raise ValueError("Options 'sort' requires option 'select'")
