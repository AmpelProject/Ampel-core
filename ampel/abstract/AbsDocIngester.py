#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsDocIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 28.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Any, Generic, Union
from ampel.types import T
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer


class AbsDocIngester(Generic[T], AmpelABC, AmpelBaseModel, abstract=True):

	updates_buffer: DBUpdatesBuffer

	@abstractmethod
	def ingest(self, doc: T, now: Union[int, float]) -> None:
		...

	def get_stats(self, reset: bool = True) -> Optional[Dict[str, Any]]:
		return None
