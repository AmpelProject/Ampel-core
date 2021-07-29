#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/MongoT3Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.05.2021
# Last Modified Date: 30.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import UpdateOne
from typing import Dict, Any, Union
from ampel.enum.DocumentCode import DocumentCode
from ampel.content.T3Document import T3Document
from ampel.mongo.utils import maybe_use_each
from ampel.abstract.AbsDocIngester import AbsDocIngester


class MongoT3Ingester(AbsDocIngester[T3Document]):

	def ingest(self, doc: T3Document, now: Union[int, float]) -> None:
		pass
