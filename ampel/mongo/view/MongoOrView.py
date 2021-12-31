#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/view/MongoOrView.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.03.2021
# Last Modified Date:  21.07.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.mongo.view.AbsMongoFlatMultiView import AbsMongoFlatMultiView

class MongoOrView(AbsMongoFlatMultiView):

	# Override to add other matching criteria (ex: tag-based selection)
	def get_first_match(self) -> dict[str, Any]:
		return {'$match': {'channel': {'$in': self.channel}}}
