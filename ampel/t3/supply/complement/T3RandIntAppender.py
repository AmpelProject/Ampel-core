#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/supply/complement/T3RandIntAppender.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.06.2020
# Last Modified Date:  14.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from random import randint
from typing import Iterable
from ampel.view.T3Store import T3Store
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.abstract.AbsBufferComplement import AbsBufferComplement


class T3RandIntAppender(AbsBufferComplement):
	"""
	Subclass of ContextUnit because implementing classes might need access to
	the AmpelConfig (foremost to the contained resource definitions)
	"""

	int_from: int = 0
	int_to: int = 100

	def complement(self, it: Iterable[AmpelBuffer], t3s: T3Store) -> None:

		for b in it:

			if b.get('extra', None):
				if isinstance(b['extra'], dict):
					if 'randint' in b['extra']:
						self.logger.error("Randint already defined in ampel buffer", extra=b) # type: ignore[arg-type]
				else:
					raise ValueError("AmpelBuffer 'extra' should be a dict")
			else:
				b['extra'] = {}

			b['extra']['randint'] = randint(self.int_from, self.int_to)  # type: ignore[index]
