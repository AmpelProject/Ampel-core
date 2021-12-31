#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/model/AmpelColModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 11.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Union, Sequence
from ampel.mongo.model.IndexModel import IndexModel
from ampel.mongo.model.ShortIndexModel import ShortIndexModel
from ampel.base.AmpelBaseModel import AmpelBaseModel

class AmpelColModel(AmpelBaseModel):
	name: str
	indexes: Optional[Sequence[Union[ShortIndexModel, IndexModel]]] = None
	args: dict = {}
