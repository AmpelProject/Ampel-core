#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/compile/StateT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2020
# Last Modified Date: 01.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.ingest.AbsStateT2Compiler import AbsStateT2Compiler


class StateT2Compiler(AbsStateT2Compiler):
	"""
	Helper class capabable of generating a nested dict that is used as basis to create T2 documents.
	The generated structure is optimized: multiple T2 documents associated with the same stock record
	accross different channels are merged into one single T2 document
	that references all corrsponding channels.
	"""

	# Not implemented yet
	pass
