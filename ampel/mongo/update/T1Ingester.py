#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/T1Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.04.2021
# Last Modified Date: 24.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib
from time import time
from pymongo import UpdateOne
from typing import Sequence, List, Tuple, Union, Set, Optional
from ampel.type import StockId, ChannelId
from ampel.log.AmpelLogger import AmpelLogger, INFO
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.content.T1Document import T1Document
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsT1Ingester import AbsT1Ingester
from ampel.abstract.AbsT1Unit import AbsT1Unit
from ampel.model.UnitModel import UnitModel
from ampel.compile.T1Compiler import T1Compiler


class T1Ingester(AbsT1Ingester[T1Compiler]):
	"""
	This class generates `T1Document` to be inserted into the t1 collection.
	Additionally, it returns a `T1Compiler` instance (method ingest) which contains the
	necessary information to generate the t2 documents to be associated with the created states.
	Used abbreviations: "eid": effective id, "sid": strict id, "chan": channel name, "comp": compound
	"""

	#: underlying T1 unit
	combiner: Union[UnitModel, str]

	#: potential hash customization
	inner_hash_alg: str = "sha256"
	outer_hash_alg: str = "sha256"

	#: true: datapoints will have the same hash regardless of their order (unordered set)
	#: false: order of datapoints will influence the computed hash
	sort: bool = True


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.channels: Set[ChannelId] = set()

		# This ingester uses a T1 unit underneath, which requires a logger
		# like every other base units. We create a logger associated a
		# buffering handlers whose logs are later transfered to the
		# LogsBufferDict instance (self.logd) used and shared among ingesters
		logger = AmpelLogger.get_logger(console=False)
		self.rbh = RecordBufferingHandler(level=INFO)
		self.rec_buffer = self.rbh.buffer
		logger.addHandler(self.rbh)
		chans = self.context.config.get('channel', dict, raise_exc=True).values()

		self.t1_unit = self.context.loader.new_base_unit(
			unit_model = self.combiner if isinstance(self.combiner, UnitModel) else UnitModel(unit=self.combiner),
			logger = logger,
			sub_type = AbsT1Unit,
			access = {chan['channel']: chan['access'] for chan in chans},
			policy = {chan['channel']: chan['policy'] for chan in chans}
		)

		self.inner_hash = getattr(hashlib, self.inner_hash_alg)
		self.outer_hash = getattr(hashlib, self.outer_hash_alg)


	def add_channel(self, channel: ChannelId):
		self.channels.add(channel)


	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> Optional[T1Compiler]:
		"""
		This method is called by the AlertProcessor for alerts
		passing at least one T0 channel filter
		"""

		# Keep only channels requiring the creation of 'states'
		chans = [k for k, v in chan_selection if k in self.channels]

		# Compute 'compound blueprint' (used for creating compounds and t2 docs)
		t1_res = self.t1_unit.combine(datapoints, chans)

		if self.rec_buffer:
			self.log_records_to_logd(self.rbh)

		compiler = T1Compiler(
			t1_res, self.inner_hash, self.outer_hash,
			self.sort, self.t1_unit.logger # type: ignore[arg-type]
		)

		# See how many different eff_comp_id were generated (possibly a single one)
		# and generate corresponding ampel document to be inserted later
		for eff_comp_id in compiler.get_effids_for_chans(chans):

			d_addtoset = {
				'channel': {
					'$each': list(
						compiler.get_chans_with_effid(eff_comp_id)
					)
				},
				'run': self.run_id
			}

			if compiler.has_flavors(eff_comp_id):
				d_addtoset['flavor'] = {
					'$each': compiler.get_compound_flavors(eff_comp_id)
				}

			comp_dict = compiler.get_eff_compound(eff_comp_id)

			comp_set_on_ins: T1Document = {
				'_id': eff_comp_id,
				'stock': stock_id,
				'tag': list(compiler.get_doc_tags(eff_comp_id)),
				'tier': 0,
				'added': time(),
				'len': len(comp_dict),
				'body': comp_dict
			}

			self.updates_buffer.add_t1_update(
				UpdateOne(
					{'_id': eff_comp_id},
					{
						'$setOnInsert': comp_set_on_ins,
						'$addToSet': d_addtoset
					},
					upsert=True
				)
			)

		return compiler
