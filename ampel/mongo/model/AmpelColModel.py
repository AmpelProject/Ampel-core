#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/AmpelColModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional, Dict, Union
from ampel.mongo.model.IndexModel import IndexModel
from ampel.mongo.model.ShortIndexModel import ShortIndexModel
from ampel.model.StrictModel import StrictModel

class AmpelColModel(StrictModel):
	name: str
	indexes: Optional[List[Union[ShortIndexModel, IndexModel]]]
	args: Optional[Dict]
