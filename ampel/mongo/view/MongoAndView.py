#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/MongoAndView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 06.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict
from ampel.mongo.view.AbsMongoFlatMultiView import AbsMongoFlatMultiView

class MongoAndView(AbsMongoFlatMultiView):
	"""
	Note that althought we select ampel documents associated with *all* the underlying channels
	(based on root key 'channel') defining this view, we do not require an "AND" connection
	for the channel field of meta records (because it makes little sense)
	"""

	# Override to add other matching criteria (ex: tag-based selection)
	def get_first_match(self) -> Dict[str, Any]:
		return {'$match': {'channel': {'$all': self.channel}}}
