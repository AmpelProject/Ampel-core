#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/abstract/AbsDataAccessController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.02.2019
# Last Modified Date: 22.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsDataAccessController(metaclass=AmpelABC):
	"""
	"""
	@classmethod
	@abstractmethod
	def get_photodata(cls, channel, photodata):
		pass
