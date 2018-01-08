#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/AlDocTypes.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 01.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class AlDocTypes():
	"""
		AlDocTypes: Ampel Document Types.
		Flag used to identify document types in the main ampel collection.
		(Majoritarily used by the mongo $match stage of the aggregation pipeline)
		DB field name: alDocType
	"""
	PHOTOPOINT					= 1
	COMPOUND					= 2
	TRANSIENT					= 3
	T2_RECORD					= 4
