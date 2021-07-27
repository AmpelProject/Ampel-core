#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/MongoAndView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 21.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict
from ampel.mongo.view.AbsMongoFlatMultiView import AbsMongoFlatMultiView

class MongoAndView(AbsMongoFlatMultiView):

	# Override to add other matching criteria (ex: tag-based selection)
	def get_first_match(self) -> Dict[str, Any]:
		return {'$match': {'channel': {'$all': self.channel}}}
