#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/config/channel/T1UnitConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.12.2018
# Last Modified Date: 07.12.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union
from ampel.common.docstringutils import gendocstring
from ampel.config.AmpelBaseModel import AmpelBaseModel

@gendocstring
class T1UnitConfig(AmpelBaseModel):
	"""
	Config holder for T1 units
	"""

	unitId: str
	runConfig: Union[None, Dict[str, Any]] = None
