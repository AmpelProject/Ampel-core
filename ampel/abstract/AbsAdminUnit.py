#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsAdminUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 16.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base import AmpelABC, AmpelUnit
from ampel.core.AmpelContext import AmpelContext


class AbsAdminUnit(AmpelABC, AmpelUnit, abstract=True):
	""" Top level abstract class containing a handle to an AmpelContext instance """

	context: AmpelContext
	verbose: int = 0
