#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/ingest/StockCompiler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.05.2021
# Last Modified Date:  21.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ujson import encode
from typing import Any
from ampel.types import ChannelId, Tag, StockId
from ampel.content.StockDocument import StockDocument
from ampel.content.JournalRecord import JournalRecord
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsCompiler import AbsCompiler
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.base.AuxUnitRegister import AuxUnitRegister


class StockCompiler(AbsCompiler):
	"""
	Compiles updates to stock document arising at different ingester stages
	Note: mypy ignores are required as of May 2021 because mypy does not
	support generic typed dict (#3863) and higher kind typevars (#548)
	"""

	id_mapper: None | str


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.register: dict[StockId, dict[str, Any]] = {}
		self._id_mapper = AuxUnitRegister.get_aux_class(
			self.id_mapper, sub_type=AbsIdMapper
		) if self.id_mapper else None


	# Override
	def add(self, # type: ignore[override]
		stock: StockId,
		channel: ChannelId,
		journal: None | JournalRecord = None,
		tag: None | Tag | list[Tag] = None
	) -> None:

		if stock in self.register:
			d = self.register[stock]
			d['channel'].add(channel)
		else:
			d = self.register[stock] = {'stock': stock, 'channel': {channel}}
			if self._id_mapper:
				d['name'] = [self._id_mapper.to_ext_id(d['stock'])]

		if journal:
			# try to merge identical journal entries with each other
			# cannot use frozenset(items) because of potential nested dicts
			k = encode(journal)
			if 'journal' in d:
				if k in d['journal']:
					d['journal'][k][1].add(channel)
				else:
					d['journal'][k] = journal, {channel}
			else:
				d['journal'] = {k: (journal, {channel})}

		if tag:
			if 'tag' not in d:
				d['tag'] = set()
			if isinstance(tag, (str, int)):
				d['tag'].add(tag)
			else:
				d['tag'].update(tag)


	# Override
	def commit(self,
		ingester: AbsDocIngester[StockDocument],
		now: int | float,
		**kwargs
	) -> None:
		"""
		Note that we let the ingester handle 'tied' and 'upd' time values
		"""

		for k, v in self.register.items():

			d: StockDocument = {
				'stock': v['stock'],
				'channel': list(v['channel'])
			}

			if self._tag:
				if 'tag' in v:
					d['tag'] = self._tag + list(v['tag'])
				else:
					d['tag'] = self._tag
			elif 'tag' in v:
				d['tag'] = v['tag']

			if 'name' in v:
				d['name'] = v['name']

			if kwargs.get('body'):
				d['body'] = kwargs['body']

			if self.origin:
				d['origin'] = self.origin

			if 'journal' in v:
				d['journal'] = [
					{
						'ts': now, 'run': self.run_id,
						'tier': self.tier, 'channel': list(chans)
					} | entry
					for entry, chans in v['journal'].values()
				]

			ingester.ingest(d)

		self.register.clear()
