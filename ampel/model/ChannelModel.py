#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ChannelModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List, Optional, Dict, Any
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.purge.PurgeModel import PurgeModel
#from ampel.model.ViewModel import ViewModel


class ChannelModel(AmpelBaseModel):

	channel: Union[int, str]
	version: Union[int, float, str]
	purge: PurgeModel = PurgeModel(
		content={'delay': 100, 'format': 'json', 'unify': True}, # type: ignore[arg-type]
		logs={'delay': 50, 'format': 'csv'} # type: ignore[arg-type]
	) 
	# view: str = "MongoChannelView"
	active: bool = True
	hash: Optional[int]
	distrib: Optional[str]
	source: Optional[str]
	contact: Optional[str]
	access: List[str] = []
	#: Identities allowed to access data associated with this channel
	members: Optional[List[str]]
	policy: List[str] = []

