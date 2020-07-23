#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/complement/T3LegacyExtJournalAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.06.2020
# Last Modified Date: 17.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, List
from ampel.type import StockId
from ampel.content.JournalRecord import JournalRecord
from ampel.t3.complement.T3ExtJournalAppender import T3ExtJournalAppender
from ampel.ztf.legacy_utils import to_ampel_id as legacy_to_ampel_id
from ampel.ztf.utils import to_ztf_id


class T3LegacyExtJournalAppender(T3ExtJournalAppender):
	""" Allows to import journal entries from a v0.6.x ampel DB """


	def get_ext_journal(self, stock_id: StockId) -> Optional[List[JournalRecord]]:
		"""
		Particularities:
		- converts stock id into the old encoding to perform DB search
		- rename field 'dt' into 'ts' to allow sorting by timestamp
		"""

		if ext_stock := next(
			self.col.find(
				{'_id': legacy_to_ampel_id(to_ztf_id(stock_id))} # type: ignore[arg-type]
			), None
		):
			for j in ext_stock['journal']:
				j['ts'] = j.pop('dt')
			if self.journal_filter:
				return self.journal_filter.apply(ext_stock['journal'])
			return ext_stock['journal']

		return None
