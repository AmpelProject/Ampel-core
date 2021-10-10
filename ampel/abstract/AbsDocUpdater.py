#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsDocUpdater.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.05.2021
# Last Modified Date: 10.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Literal
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel


class AbsDocUpdater(AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Aim at facilating updates of documents regardless of the underlying database system.
	Not in use yet
	"""

	@abstractmethod
	def update(self,
		tier: Literal[0, 1, 2],
		match: Dict[str, Any],
		let: Dict[str, Any],
		push: Dict[str, Any],
		add_to_set: Dict[str, Any]
	) -> None:
		...
