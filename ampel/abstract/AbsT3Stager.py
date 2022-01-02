#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsT3Stager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                04.01.2020
# Last Modified Date:  10.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator

from ampel.view.T3Store import T3Store
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.core.ContextUnit import ContextUnit


class AbsT3Stager(AmpelABC, ContextUnit, abstract=True):
	""" Supply stock views to one or more T3 units.  """

	#: number of buffers to process at once. Set to 0 to disable chunking
	chunk_size: int = 1000

	#: Cast ampel buffers into views for each t3 unit (meaning possibly redundantly)
	#: since there is no real read-only struct in python
	paranoia: bool = True


	@abstractmethod
	def stage(self,
		data: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:
		""" Process a chunk of AmpelBuffer instances """
