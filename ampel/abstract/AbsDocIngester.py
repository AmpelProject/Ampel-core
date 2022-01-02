#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsDocIngester.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.03.2020
# Last Modified Date:  09.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Generic
from ampel.types import T
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer


class AbsDocIngester(Generic[T], AmpelABC, AmpelBaseModel, abstract=True):

	updates_buffer: DBUpdatesBuffer

	@abstractmethod
	def ingest(self, doc: T) -> None:
		...

	def get_stats(self, reset: bool = True) -> None | dict[str, Any]:
		return None
