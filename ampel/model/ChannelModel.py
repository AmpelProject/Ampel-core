#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ChannelModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.10.2019
# Last Modified Date:  22.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Sequence

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.purge.PurgeModel import PurgeModel

#from ampel.model.ViewModel import ViewModel


class ChannelModel(AmpelBaseModel):

	channel: int | str
	version: None | int | float | str
	purge: PurgeModel = PurgeModel(
		content={'delay': {'days': 100}, 'format': 'json', 'unify': True}, # type: ignore[arg-type]
		logs={'delay': 50, 'format': 'csv'} # type: ignore[arg-type]
	)
	# view: str = "MongoChannelView"
	active: bool = True
	hash: None | int = None
	distrib: None | str = None
	source: None | str = None
	contact: None | str = None
	access: Sequence[str] = []
	#: Identities allowed to access data associated with this channel
	members: None | Sequence[str] = None
	policy: Sequence[str] = []
