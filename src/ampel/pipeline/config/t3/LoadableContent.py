#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3LoadableDocs.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Enum

class T3LoadableDocs(str, Enum):
	""" """
	TRANSIENT = "TRANSIENT"
	PHOTOPOINT = "PHOTOPOINT"
	UPPERLIMIT = "UPPERLIMIT"
	COMPOUND = "COMPOUND"
	T2RECORD = "T2RECORD"
