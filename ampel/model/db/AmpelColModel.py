#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/db/AmpelColModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Optional, Dict, ClassVar
from ampel.model.db.IndexModel import IndexModel
from ampel.config.pydantic import BetterConfigDefaults


class AmpelColModel(BaseModel):
	""" """
	Config: ClassVar = BetterConfigDefaults # type: ignore

	name: str
	indexes: Optional[List[IndexModel]]
	args: Optional[Dict] = None
