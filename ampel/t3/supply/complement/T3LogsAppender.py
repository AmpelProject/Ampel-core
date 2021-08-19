#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/supply/complement/T3LogsAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.03.2021
# Last Modified Date: 15.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime
from bson.objectid import ObjectId
from typing import Iterable, Dict, Any
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.core.AmpelContext import AmpelContext
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.mongo.query.var.LogsLoader import LogsLoader
from ampel.log.utils import safe_query_dict
from ampel.abstract.AbsBufferComplement import AbsBufferComplement


class T3LogsAppender(AbsBufferComplement):

	use_last_run: bool = True
	logs_loader_conf: Dict[str, Any] = {}

	def __init__(self, context: AmpelContext, **kwargs) -> None:

		AmpelBaseModel.__init__(self, **kwargs)
		self.log_loader = LogsLoader(**self.logs_loader_conf, read_only=True)
		self.query = {}
		self.col = context.db.get_collection('logs')
		if self.session_info and self.use_last_run and self.session_info.get('last_run'):
			self.query['_id'] = {
				'$gte': ObjectId.from_datetime(
					datetime.utcfromtimestamp(
						self.session_info['last_run']
					)
				)
			}


	def complement(self, it: Iterable[AmpelBuffer]) -> None:

		self.query['s'] = {'$in': [el['id'] for el in it]}
		logs = list(self.log_loader.fetch_logs(self.col, self.query))

		self.logger.debug(
			f"Log query returned {len(logs)} result(s)",
			extra=safe_query_dict(self.query)
		)

		if not logs:
			return

		for ab in it:
			if 'logs' not in ab or ab['logs'] is None:
				ab['logs'] = []
			for l in logs:
				if ab['id'] == l['s']:
					ab['logs'].append(l) # type: ignore
