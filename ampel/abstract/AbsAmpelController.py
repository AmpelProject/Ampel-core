#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAmpelController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.10.2019
# Last Modified Date: 19.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from ampel.model.ProcessData import ProcessData
from ampel.config.AmpelConfig import AmpelConfig
from ampel.abstract.AmpelABC import AmpelABC

class AbsAmpelController(metaclass=AmpelABC):
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

	def schedule(self, proc_conf: ProcessData) -> None:
		""" """
