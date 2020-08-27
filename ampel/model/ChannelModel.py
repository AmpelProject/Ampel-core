#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ChannelModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List, Optional, Dict, Any
from ampel.model.StrictModel import StrictModel
from ampel.model.purge.PurgeModel import PurgeModel


class ChannelModel(StrictModel):

	channel: Union[int, str]
	purge: PurgeModel = {
		'content': {'delay': 100, 'format': 'json', 'unify': True},
		'logs': {'delay': 50, 'format': 'csv'}
	} # type: ignore[assignment]
	active: bool = True
	hash: Optional[int]
	distrib: Optional[str]
	source: Optional[str]
	contact: Optional[str]
	access: Optional[List[str]]
	policy: List[str] = []


	def dict(self, **kwargs) -> Dict[str, Any]:
		if 'exclude_defaults' not in kwargs:
			kwargs['exclude_defaults'] = False
		return super().dict(**kwargs)
