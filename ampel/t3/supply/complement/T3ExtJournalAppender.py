#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/supply/complement/T3ExtJournalAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.06.2020
# Last Modified Date: 14.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import MongoClient
from typing import Iterable, Optional, Union, List
from ampel.types import StockId
from ampel.aux.filter.SimpleDictArrayFilter import SimpleDictArrayFilter
from ampel.content.JournalRecord import JournalRecord
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.abstract.AbsBufferComplement import AbsBufferComplement
from ampel.model.aux.FilterCriterion import FilterCriterion
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.FlatAnyOf import FlatAnyOf
from ampel.view.T3Store import T3Store


class T3ExtJournalAppender(AbsBufferComplement):
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


	def __init__(self, **kwargs) -> None:

		super.__init__(**kwargs)

		if self.filter_config:
			self.journal_filter: SimpleDictArrayFilter[JournalRecord] = SimpleDictArrayFilter(filters=self.filter_config)

		self.col = MongoClient(self.context.config.get(f'resource.{self.mongo_resource}')) \
			.get_database(self.db_name)\
			.get_collection("stock")


	def get_ext_journal(self, stock_id: StockId) -> Optional[List[JournalRecord]]:

		if ext_stock := next(self.col.find({'_id': stock_id}), None):
			if self.journal_filter:
				return self.journal_filter.apply(ext_stock['journal'])
			return ext_stock['journal']
		return None


	def complement(self, it: Iterable[AmpelBuffer], t3s: T3Store) -> None:

		for albuf in it:

			if 'stock' in albuf and isinstance(albuf['stock'], dict):

				if entries := self.get_ext_journal(albuf['stock']['stock']):

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
