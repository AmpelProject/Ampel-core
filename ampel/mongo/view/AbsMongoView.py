#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/MongoChannelView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 26.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Any, Dict
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel


class AbsMongoView(AmpelABC, AmpelBaseModel, abstract=True):

	@abstractmethod
	def stock(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t0(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t1(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t2(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t3(self) -> List[Dict[str, Any]]:
		...
