#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/select/AbsT3Selector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 17.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable, ClassVar, Optional
from ampel.base import abstractmethod
from ampel.core.AdminUnit import AdminUnit


class AbsT3Selector(AdminUnit, abstract=True):

	#: field used to identify stocks
	field_name: ClassVar[str] = "_id"

	@abstractmethod
	def fetch(self) -> Optional[Iterable]:
		"""
		Get selected stock ids
		"""
		...
