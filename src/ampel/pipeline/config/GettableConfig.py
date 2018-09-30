#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/GettableConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.common.AmpelUtils import AmpelUtils

class GettableConfig:

	def get(self, path):
		return AmpelUtils.get_nested_attr(self, path)
