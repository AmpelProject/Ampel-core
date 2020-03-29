#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsControllerUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 18.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from ampel.model.ProcessModel import ProcessModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.abc.AmpelABC import AmpelABC

class AbsControllerUnit(AmpelABC, abstract=True):
	"""
	"""

	def __init__(self, tier: int, path_to_config: str):
		""" """
		self.tier = tier
		self.ampel_config = AmpelConfig(
			json.load(
				open(path_to_config, "r")
			)
		)

	def schedule(self, proc_conf: ProcessModel) -> None:
		""" """
