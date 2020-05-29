#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/PlainUnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 04.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Any
from ampel.model.AmpelStrictModel import AmpelStrictModel


class PlainUnitModel(AmpelStrictModel):
	unit: str
	config: Optional[Dict[str, Any]]
