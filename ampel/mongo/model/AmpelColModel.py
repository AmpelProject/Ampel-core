#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/model/AmpelColModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 11.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional, Union
from ampel.mongo.model.IndexModel import IndexModel
from ampel.mongo.model.ShortIndexModel import ShortIndexModel
from ampel.model.StrictModel import StrictModel

class AmpelColModel(StrictModel):
	name: str
	indexes: Optional[List[Union[ShortIndexModel, IndexModel]]]
	args: dict = {}
