#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/supply/T3DefaultSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.07.2021
# Last Modified Date: 15.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional, Sequence, Generator
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.abstract.AbsT3Selector import AbsT3Selector
from ampel.abstract.AbsT3Loader import AbsT3Loader
from ampel.abstract.AbsBufferComplement import AbsBufferComplement
from ampel.util.collections import chunks as chunks_func
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.model.UnitModel import UnitModel
from ampel.log.utils import report_exception


class T3DefaultSupplier(AbsT3Supplier):
	"""
	Default T3 supplier that:
	- matches stock ids based on the stock collection (section 'select'),
	- loads AmpelBuffer content as defined in the 'load' section
	- potentially updates AmpelBuffer through the 'complement' section
	"""

	#: Select stocks
	select: UnitModel

	#: Fill :class:`~ampel.core.AmpelBuffer.AmpelBuffer` for each selected stock
	load: UnitModel

	#: Add external information to each :class:`~ampel.core.AmpelBuffer.AmpelBuffer`.
	complement: Optional[Sequence[UnitModel]]

	#: number of stocks to load at once. Set to 0 to disable chunking
	chunk_size: int = 1000


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		# stock selection
		#################

		# Spawn and run a new selector instance
		# fetch() returns an iterable (often a pymongo cursor)
		self.selector = self.context.loader.new_context_unit(
			model = self.select,
			context = self.context,
			sub_type = AbsT3Selector,
			logger = self.logger
		)

		# Content loader
		################

		# Spawn requested content loader
		self.data_loader = self.context.loader \
			.new_context_unit(
				model = self.load,
				context = self.context,
				sub_type = AbsT3Loader,
				logger = self.logger
			)

		# Content complementer
		######################

		if self.complement:

			# Spawn requested snapdata complementers
			self.complementers: Optional[List[AbsBufferComplement]] = [
				self.context.loader \
					.new_context_unit(
						model = conf_el,
						context = self.context,
						sub_type = AbsBufferComplement,
						logger = self.logger
					)
				for conf_el in self.complement
			]
		else:
			self.complementers = None


	def supply(self) -> Generator[AmpelBuffer, None, None]:

		# NB: we consume the entire cursor at once using list() to be robust
		# against cursor timeouts or server restarts during long lived T3 processes
		stock_ids = list(self.selector.fetch() or [])
		if not stock_ids:
			raise StopIteration

		# Usually, id_key is '_id' but it can be 'stock' if the
		# selection is based on t2 documents for example
		id_key = self.selector.field_name

		# Run start
		###########
		chunks = chunks_func(stock_ids, self.chunk_size) if self.chunk_size > 0 else [stock_ids]

		# Loop over chunks from the cursor/iterator
		for chunk_ids in chunks:

			# allow working chunks to complete even if some raise exception
			try:

				# Load info from DB
				tran_data = self.data_loader.load([sid[id_key] for sid in chunk_ids])

				# Potentialy add complementary information (spectra, TNS names, ...)
				if self.complementers:
					for appender in self.complementers:
						appender.complement(tran_data)

				for ampel_buffer in tran_data:
					yield ampel_buffer

			except Exception as e:

				if self.event_hdlr:
					self.event_hdlr.add_extra(self.logger, success=False)

				if self.raise_exc:
					raise e

				report_exception(
					self.context.db, self.logger, exc=e,
					info={'process': self.process_name}
				)
