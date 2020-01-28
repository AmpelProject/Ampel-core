#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : src/ampel/model/BetterConfigDefaults.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.10.2019
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseConfig, Extra

class BetterConfigDefaults(BaseConfig):
	"""
	Pydantic settings that should be activated by default
	"""

	arbitrary_types_allowed=True
	extra = Extra.forbid
