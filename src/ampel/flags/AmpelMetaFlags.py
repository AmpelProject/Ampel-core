#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/AmpelMetaFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntFlag
from ampel.flags.AmpelFlags import AmpelFlags

class AmpelMetaFlags(type):             

	def __new__(metacls, name, bases, d):
		flags = [(i.name, i.value) for i in AmpelFlags]
		for k,v in d.items():
			if not k.endswith('__'):
				flags.append((k,v))
		return IntFlag(name, flags)
