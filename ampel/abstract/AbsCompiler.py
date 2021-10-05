#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsCompiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.05.2021
# Last Modified Date: 01.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Literal, Sequence, Union, List, Set
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


	def merge_tags(self,
		tag1: Union[Tag, Union[List[Tag], Set[Tag]]],
		tag2: Union[Tag, Union[List[Tag], Set[Tag]]]
	) -> Union[Tag, List[Tag]]:

		if isinstance(tag1, (int, str)):
			if isinstance(tag2, (int, str)):
				if tag2 != tag1:
					return [tag1, tag2]
				return tag1
			else:
				if tag1 in tag2:
					return tag2 if isinstance(tag2, list) else list(tag2)
				l = list(tag2)
				l.append(tag1)
				return l
		else:
			if isinstance(tag2, (int, str)):
				if tag2 in tag1:
					return tag1 if isinstance(tag1, list) else list(tag1)
				l = list(tag1)
				l.append(tag2)
				return l
			else:
				s = tag1 if isinstance(tag1, set) else set(tag1)
				s.update(tag2)
				return list(s)


	@abstractmethod(var_args=True)
	def add(self) -> None:
		...

	@abstractmethod
	def commit(self, ingester: AbsDocIngester, now: Union[int, float]) -> None:
		...
