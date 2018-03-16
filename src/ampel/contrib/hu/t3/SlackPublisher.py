#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : SlackPublisher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.03.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsT3Unit import AbsT3Unit

class SlackPublisher(AbsT3Unit):

	version = 0.1

	def __init__(self, logger, base_config=None):
		pass

	def run(self, run_config, transients=None):
		pass
