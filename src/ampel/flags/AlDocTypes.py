#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/AlDocTypes.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 18.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntFlag

class AlDocTypes(IntFlag):
	"""
	AlDocTypes: Ampel Document Types.
	Flag used to identify document types in the main ampel collection.
	(Majoritarily used by the mongo $match stage of the aggregation pipeline)
	DB field name: alDocType

	IntFlag is used rather than Flag in order to  allow quicker syntax for comparison of the kind 
	"if AlDocTypes.PHOTOPOINT == 1" (intead of "AlDocTypes.PHOTOPOINT.value == 1"). 
	Except when it is used as options ('load_options' in TransientLoader for example), 
	flag *combinations* of AlDocTypes are never used since DB document are single typed.
	"""
	NOTYPE			= 0
	PHOTOPOINT		= 1
	COMPOUND		= 2
	TRANSIENT		= 4
	T2RECORD		= 8
