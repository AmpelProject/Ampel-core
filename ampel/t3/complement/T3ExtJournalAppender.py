#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/complement/T3ExtJournalAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.06.2020
# Last Modified Date: 17.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import MongoClient
from typing import Iterable, Optional, Union, List
from ampel.type import StockId
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.aux.filter.SimpleDictArrayFilter import SimpleDictArrayFilter
from ampel.content.JournalRecord import JournalRecord
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.t3.complement.AbsT3DataAppender import AbsT3DataAppender
from ampel.model.aux.FilterCriterion import FilterCriterion
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.FlatAnyOf import FlatAnyOf
#from ampel.abstract.AbsApplicable import AbsApplicable


class T3ExtJournalAppender(AbsT3DataAppender):
	"""
	Import journal entries from a 'foreign' database, e.g. one created
	by a previous version of Ampel.
	"""

	mongo_resource: str = "resource.ext_mongo"
	db_name: str = "Ampel_data"
	sort: bool = True
	reverse: bool = True
	filter_config: Optional[
		Union[FilterCriterion, FlatAnyOf[FilterCriterion], AllOf[FilterCriterion]]
	] = None


	def __init__(self, context: AmpelContext, **kwargs) -> None:

		AmpelBaseModel.__init__(self, **kwargs)

		if self.filter_config:
			self.journal_filter: SimpleDictArrayFilter[JournalRecord] = SimpleDictArrayFilter(filters=self.filter_config)

		self.col = MongoClient(context.config.get(f'resource.{self.mongo_resource}')) \
			.get_database(self.db_name)\
			.get_collection("stock")


	def get_ext_journal(self, stock_id: StockId) -> Optional[List[JournalRecord]]:

		if ext_stock := next(self.col.find({'_id': stock_id}), None):
			if self.journal_filter:
				return self.journal_filter.apply(ext_stock['journal'])
			return ext_stock['journal']
		return None


	def complement(self, it: Iterable[AmpelBuffer]) -> None:

		for albuf in it:

			if 'stock' in albuf and isinstance(albuf['stock'], dict):

				if entries := self.get_ext_journal(albuf['stock']['_id']):

					entries.extend(albuf['stock']['journal'])

					if self.sort:
						dict.__setitem__(
							albuf['stock'], 'journal', sorted( # type: ignore[index]
								entries, key=lambda x: x['ts'], reverse=self.reverse
							)
						)
			else:
				print(albuf)
				raise ValueError("No stock information available")
