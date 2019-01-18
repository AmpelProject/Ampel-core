#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/core/flags/ScienceRecordFlag.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.01.2019
# Last Modified Date: 18.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from abc import ABCMeta

class ScienceRecordFlag(metaclass=ABCMeta):
	"""
	"""
	HAS_ERROR              = ()
	HAS_EXCEPTION          = ()
