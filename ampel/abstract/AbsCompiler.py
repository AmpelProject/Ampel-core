#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsCompiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.05.2021
# Last Modified Date: 27.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Literal, Sequence, Union
from ampel.types import Tag
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.abstract.AbsDocIngester import AbsDocIngester


class AbsCompiler(AmpelABC, AmpelBaseModel, abstract=True):

	origin: Optional[int] = None
	tier: Literal[-1, 0, 1, 2, 3]
	run_id: int
	tag: Optional[Union[Tag, Sequence[Tag]]]


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self._tag = None
		if self.tag:
			if isinstance(self.tag, (str, int)):
				self._tag = [self.tag]
			else:
				self._tag = list(self.tag)


	@abstractmethod(var_args=True)
	def add(self) -> None:
		...

	@abstractmethod
	def commit(self, ingester: AbsDocIngester, now: Union[int, float]) -> None:
		...
