#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsCompoundItemBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.10.2019
# Last Modified Date: 22.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsCompoundItemBuilder(metaclass=AmpelABC):
	"""
	"""

	@abstractmethod
	def build(self, element: Dict[str, Any], channel_name: Union[int, str]) -> Dict[str, Any]:
		""" """
