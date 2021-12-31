#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/supply/complement/T3LogsAppender.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.03.2021
# Last Modified Date:  13.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from datetime import datetime
from bson.objectid import ObjectId
from typing import Any
from collections.abc import Iterable
from ampel.view.T3Store import T3Store
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.mongo.query.var.LogsLoader import LogsLoader
from ampel.log.utils import safe_query_dict
from ampel.abstract.AbsBufferComplement import AbsBufferComplement


class T3LogsAppender(AbsBufferComplement):

	use_last_run: bool = True
	logs_loader_conf: dict[str, Any] = {}

	def __init__(self, **kwargs) -> None:
		super.__init__(**kwargs)
		self.log_loader = LogsLoader(**self.logs_loader_conf, read_only=True)
		self.col = self.context.db.get_collection('logs')


	def complement(self, it: Iterable[AmpelBuffer], t3s: T3Store) -> None:

		query = {'s': {'$in': [el['id'] for el in it]}}

		if t3s.session and self.use_last_run and t3s.session.get('last_run'):
			query['_id'] = {
				'$gte': ObjectId.from_datetime(
					datetime.utcfromtimestamp(
						t3s.session['last_run']
					)
				)
			}

		logs = list(
			self.log_loader.fetch_logs(self.col, query)
		)

		self.logger.debug(
			f"Log query returned {len(logs)} result(s)",
			extra=safe_query_dict(query)
		)

		if not logs:
			return

		for ab in it:
			if 'logs' not in ab or ab['logs'] is None:
				ab['logs'] = []
			for l in logs:
				if ab['id'] == l['s']:
					ab['logs'].append(l) # type: ignore
