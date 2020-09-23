#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AdminUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 15.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelContext import AmpelContext


class AdminUnit(AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Top level abstract class receiving a reference to
	an AmpelContext instance as constructor parameter
	"""

	def __init__(self, context: AmpelContext, **kwargs):
		#: Configuration, database client, etc.
		self.context: AmpelContext = context
		AmpelBaseModel.__init__(self, **kwargs)
