#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/JobFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 17.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag


class JobFlags(Flag):
	"""
		Flags used by DBJobReporter when creating a document to be pushed to the collection "events".
		Since the documents, once created, are never updated (and thus the $bit operator 
		is not required), this class can embbed more than 64 different flags.
		HAS_ERROR and HAS_CRITICAL flags will be converted into a sparse indexed field named "err".
	"""
	NO_FLAG				= 0

	HAS_ERROR    		= 1 
	HAS_CRITICAL   		= 2

	T0		            = 4 
	T1		            = 8 
	T2		            = 16
	T3		            = 32 

	INST_ZTF			= 64
	INST_OTHER1			= 128
	INST_OTHER2			= 256
	INST_OTHER3			= 512

	T3_PURGE			= 1024
	T3_MARSHALL_PUSH	= 2048
	T3_JUPYTER			= 4096
	T3_RANKING			= 8192
	T3_ERROR_REPORTER	= 16384
