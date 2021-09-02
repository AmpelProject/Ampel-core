#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3ProjectingStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 28.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Optional, Sequence, List, Generator, Union, Dict, Type, Iterable
from itertools import islice
from multiprocessing import JoinableQueue
from multiprocessing.pool import ThreadPool

from ampel.types import StockId, UBson
from ampel.log import VERBOSE
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.content.T3Document import T3Document
from ampel.struct.UnitResult import UnitResult
from ampel.model.t3.T3ProjectionDirective import T3ProjectionDirective
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.abstract.AbsT3Filter import AbsT3Filter
from ampel.abstract.AbsT3Projector import AbsT3Projector
from ampel.t3.stage.SimpleViewGenerator import SimpleViewGenerator


class RunBlock:
	"""
	Used internally by T3UnitRunner
	"""
	filter: Optional[AbsT3Filter]
	projector: Optional[AbsT3Projector]
	units: List[AbsT3Unit]
	stock_ids: Optional[List[StockId]]
	qdict: Dict[Type, List[JoinableQueue]]

	def __init__(self):
		self.filter = None
		self.projector = None
		self.units = []
		self.stock_ids = None
		self.len_view_types = 0
		self.qvals = None


class T3ProjectingStager(T3BaseStager):
	"""
	Default implementation handling the instructions defined
	by :class:`T3Directive.run <ampel.model.t3.T3Directive.T3Directive>`.
	"""

	#: Processing specification
	directives: Sequence[T3ProjectionDirective]

	#: whether selected stock ids should be saved into the (potential) t3 documents
	save_stock_ids: bool = True


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.run_blocks: List[RunBlock] = []
		debug = self.logger.verbose > 1

		if debug:
			self.logger.debug("Setting up T3ProjectingStager")

		for directive in self.directives:

			rb = RunBlock()

			if directive.filter:

				if debug:
					self.logger.debug(f"Setting up filter {directive.filter.unit}")

				rb.filter = self.context.loader.new(
					model = directive.filter,
					unit_type = AbsT3Filter,
					logger = self.logger
				)

				if self.save_stock_ids:
					rb.stock_ids = []


			if directive.project:

				if debug:
					self.logger.debug(f"Setting up projector {directive.project.unit}")

				rb.projector = AuxUnitRegister.new_unit(
					model = directive.project,
					sub_type = AbsT3Projector,
					logger = self.logger
				)

			for exec_def in directive.execute:
				rb.units.append(
					self.get_unit(exec_def)
				)

			self.run_blocks.append(rb)


	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Optional[Union[T3Document, List[T3Document]]]:

		if len(self.run_blocks) == 1:
			if len(self.run_blocks[0].units) == 1:
				return self.supply(
					self.run_blocks[0].units[0],
					SimpleViewGenerator(
						self.run_blocks[0].units[0],
						self.projected_buffer_generator(self.run_blocks[0], data),
						self.stock_updr
					)
				)
			else:
				return self.multi_supply(
					self.run_blocks[0].units,
					self.projected_buffer_generator(self.run_blocks[0], data)
				)
		else:

			if self.chunk_size <= 0:
				raise ValueError("Chunking is required when multiple filter/projection blocks are defined")

			return self.multi_bla(data)
			#for gen in tee(data, len(self.run_blocks)):
			# pass

		return None


	def projected_buffer_generator(self,
		run_block: RunBlock,
		gen: Generator[AmpelBuffer, None, None]
	) -> Generator[AmpelBuffer, None, None]:
	
		# Chunk input buffers (loaded from generator)
		buffers: Sequence[AmpelBuffer]

		try:
			while (buffers := list(islice(gen, self.chunk_size))):

				if run_block.filter:

					if self.logger.verbose:
						self.logger.log(VERBOSE, "Applying run-block filter")

					buffers = run_block.filter.filter(buffers)

					if self.save_stock_ids:
						run_block.stock_ids.extend([el['id'] for el in buffers])  # type: ignore[union-attr]

				if run_block.projector:

					if self.logger.verbose:
						self.logger.log(VERBOSE, "Applying run-block projection")

					buffers = run_block.projector.project(buffers)

				for el in buffers:
					yield el

		except RuntimeError as e:
			if "StopIteration" in str(e):
				return None
			raise e



	def multi_bla(self, gen: Generator[AmpelBuffer, None, None]) -> Optional[List[T3Document]]:
		"""
		Handles multi run-blocks, that is multi projection/filters using the same input data
		"""

		all_units = []
		for rb in self.run_blocks:
			all_units += rb.units

		try:

			ts = time()
			with ThreadPool(processes=len(all_units)) as pool:

				# Create queues and generators for all instanciated t3 units
				queues, generators, async_results = self.create_threaded_generators(pool, all_units)

				# Add helper values to run_blocks
				for rb in self.run_blocks:

					# Number of view types for this this run block
					rb.len_view_types = len({t3_unit._View for t3_unit in rb.units})

					# Subset of queues for this run block
					rb.qvals = [q for u, q in queues.items() if u in rb.units]

					# Optimize by potentially grouping units associated with the same view type
					rb.qdict = {}
					for unit in rb.units:
						if unit.__class__._View not in rb.qdict:
							rb.qdict[unit.__class__._View] = []
						rb.qdict[unit.__class__._View].append(queues[unit])


				# Chunk input buffers (loaded from generator)
				while (x := list(islice(gen, self.chunk_size))):

					# Iterate over run blocks and add the projected buffers into the corresponding queues
					for rb in self.run_blocks:

						buffers: Iterable[AmpelBuffer] = x
						if rb.filter:

							if self.logger.verbose:
								self.logger.log(VERBOSE, "Applying run-block filter")

							buffers = rb.filter.filter(buffers)

							if self.save_stock_ids:
								rb.stock_ids.extend([el['id'] for el in buffers])  # type: ignore[union-attr]

						if rb.projector:

							if self.logger.verbose:
								self.logger.log(VERBOSE, "Applying run-block projection")

							buffers = rb.projector.project(buffers)

						self.put_views(buffers, rb.qdict)

					# Send sentinel all threaded generators
					for q in queues.values():
						q.put(None) # type: ignore[arg-type]

					ret: List[T3Document] = []
					for async_res, generator, t3_unit in zip(async_results, generators, all_units):

						# potential T3Record to be included in the T3Document
						if (t3_unit_result := async_res.get()):
							if (z := self.handle_t3_result(t3_unit, t3_unit_result, generator.stocks, ts)):
								ret.append(z)

				for i, rb in enumerate(self.run_blocks):
					self.flush(rb.units, extra={'directive': i})

				return ret

		except Exception as e:
			self.flush(all_units)
			self.handle_error(e)

		return None


	def craft_t3_doc(self,
		t3_unit: AbsT3Unit,
		res: Union[None, UBson, UnitResult],
		ts: float,
		stocks: Optional[List[StockId]] = None
	) -> T3Document:

		t3_doc = super().craft_t3_doc(t3_unit, res, ts, stocks)
		if self.save_stock_ids:
			rb = next(filter(lambda rb: t3_unit in rb.units, self.run_blocks))
			if rb.stock_ids:
				t3_doc['stock'] = rb.stock_ids
		return t3_doc
