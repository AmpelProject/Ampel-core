#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/builder/BuilderAliasModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from ampel.model.StrictModel import StrictModel

class BuilderAliasModel(StrictModel):

	t0: Optional[Dict[str, Any]]
	t1: Optional[Dict[str, Any]]
	t2: Optional[Dict[str, Any]]
	t3: Optional[Dict[str, Any]]
