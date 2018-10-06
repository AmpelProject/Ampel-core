#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/AlDocType.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 06.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntEnum


class AlDocType(IntEnum):
	"""
	Ampel Document Type.
	Enum members are used to identify document types in the main ampel collection.
	(Majoritarily used by the mongo $match stage of the aggregation pipeline)
	DB field name: alDocType

	IntEnum is used rather than Enum in order to  allow quicker syntax for comparison of the kind 
	"if AlDocType.PHOTOPOINT == 1" (intead of "AlDocType.PHOTOPOINT.value == 1"). 
	"""
	TRANSIENT		= 1
	PHOTOPOINT		= 2
	UPPERLIMIT		= 4
	COMPOUND		= 8
	T2RECORD		= 16
