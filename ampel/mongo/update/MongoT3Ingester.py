#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/MongoT3Ingester.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                30.05.2021
# Last Modified Date:  30.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.T3Document import T3Document


class MongoT3Ingester(AbsDocIngester[T3Document]):

	def ingest(self, doc: T3Document) -> None:
		raise NotImplementedError("Soon...")
