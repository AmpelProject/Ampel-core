#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/TransientFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AmpelMetaFlags import AmpelMetaFlags

class TransientFlags(metaclass=AmpelMetaFlags):
	"""
	First 20 powers of two are reserved for general ampel flags
	"""
	HAS_ERROR                   = 1048576
	T1_AUTO_COMPLETE            = 2097152
