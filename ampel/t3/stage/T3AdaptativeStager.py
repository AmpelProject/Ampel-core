#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3AdaptativeStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 09.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from itertools import islice
from multiprocessing import JoinableQueue
from multiprocessing.pool import ThreadPool, AsyncResult
from typing import Dict, Optional, Sequence, Set, List, Generator

from ampel.types import ChannelId
from ampel.view.T3Store import T3Store
from ampel.view.SnapView import SnapView
from ampel.model.UnitModel import UnitModel
from ampel.content.T3Document import T3Document
from ampel.log import VERBOSE
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.abstract.AbsT3Filter import AbsT3Filter
from ampel.abstract.AbsT3Projector import AbsT3Projector
from ampel.t3.stage.T3ThreadedStager import T3ThreadedStager
from ampel.t3.stage.T3ProjectingStager import RunBlock
from ampel.t3.stage.ThreadedViewGenerator import ThreadedViewGenerator


class T3AdaptativeStager(T3ThreadedStager):
	"""
	Unit stager that for each channel found in the elements loaded by the previous stages:
	
	- spawns a dedicated :class:`~ampel.t3.run.T3ProjectingStager.T3ProjectingStager` instance configured to filter and project elements wrt this channel
	- execute the associated T3 units

	Example:
	A general T3 process performs a broad, channel-less query.
	Many stocks are returned, each possibly associated with different channels.
	This unit builds a set of all channels referenced by the results.
	Then, for each channel, the information about the referenced elements are posted into
	a dedicated Slack channel whose name corresponds to the Ampel channel.
	Suppose, for example, that the results contain objects:

	- ``<object 1 with channel {A}>``
	- ``<object 2 with channel {A, B}>``
	- ``<object 3 with channel {B, C, D}>``

	This will cause:
	
	- Object 1 and 2 to be posted to slack channel #A
	- Object 2 and 3  to be posted to slack channel #B
	- Object 3 to be posted to slack channel #C
	- Object 3 to be posted to slack channel #D
	"""

	execute: Sequence[UnitModel]
	white_list: Optional[Set[str]] = None
	black_list: Optional[Set[str]] = None

	remove_empty: bool = True
	unalterable: bool = True
	freeze: bool = False

	nthread: int = 4


	def __init__(self, **kwargs) -> None:
		"""
		:param execute: sequence of T3 units to be executed
		:param white_list: perform operations only for channels referenced in this list
		:param black_list: do nothing for channels referenced in this list
		:param channel: (from super class T3UnitRunner): if set, all log entries produced
		by the underlying t3 units will be associated with the provided channel.
		Otherwise, log entries will be associated with the projected channel.
		:param remove_empty: see T3BaseProjector docstring
		:param unalterable: see T3ChannelProjector docstring
		:param freeze: see T3ChannelProjector docstring
		"""

		super().__init__(**kwargs)
		self.run_blocks: Dict[ChannelId, RunBlock] = {}

		if self.logger.verbose > 1 and (self.white_list or self.black_list):
			self.warned: Set[ChannelId] = set()

		if self.black_list and self.white_list:
			raise ValueError("Can't have both black and white lists")

		self.queues: Dict[AbsT3ReviewUnit, JoinableQueue[SnapView]] = {}
		self.generators: List[ThreadedViewGenerator] = []
		self.async_results: List[AsyncResult] = []


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> Optional[Generator[T3Document, None, None]]:

		ts = time()
		with ThreadPool(processes=self.nthread) as pool:

			# Chunk input buffers (loaded from generator)
			while (data := list(islice(gen, self.chunk_size))):

				# step 1: Gather all channels
				# (will raise Error if stock documents were to requested to be loaded)
				channels = self.filter_channels({
					ell for el in data for ell in el['stock']['channel'] # type: ignore[index]
				})

				# step 2: spawn T3UnitRunner instances
				for chan in channels:

					if chan in self.run_blocks:
						rb = self.run_blocks[chan]

					else:

						rb = RunBlock()
						rb.units = [self.get_unit(um, chan=chan) for um in self.execute]

						# Create and start T3 units "process(...)" threads (generator will block)
						qs, gs, rs = self.create_threaded_generators(pool, rb.units, t3s)

						self.queues.update(qs)
						self.generators.extend(gs)
						self.async_results.extend(rs)

						rb.filter = self.context.loader.new(
							model = UnitModel(unit="T3AmpelBufferFilter", config={"channel": chan}),
							unit_type = AbsT3Filter,
							logger = self.logger
						)

						if self.save_stock_ids:
							rb.stock_ids = []

						rb.projector = AuxUnitRegister.new_unit(
							model = UnitModel(
								unit="T3ChannelProjector",
								config={"channel": chan, "remove_empty": self.remove_empty}
							),
							sub_type = AbsT3Projector,
							logger = self.logger
						)

						# Dict used to potentially optimize views generation
						rb.qdict = {}
						for unit in rb.units:
							if unit.__class__._View not in rb.qdict:
								rb.qdict[unit.__class__._View] = []
							rb.qdict[unit.__class__._View].append(qs[unit])

						self.run_blocks[chan] = rb

					buffers: Sequence[AmpelBuffer] = data

					if self.logger.verbose:
						self.logger.log(VERBOSE, "Applying run-block filter")

					buffers = rb.filter.filter(buffers) # type: ignore[union-attr]

					if self.save_stock_ids:
						rb.stock_ids.extend([el['id'] for el in buffers])  # type: ignore[union-attr]

					if self.logger.verbose:
						self.logger.log(VERBOSE, "Applying run-block projection")

					buffers = rb.projector.project(buffers) # type: ignore[union-attr]

					self.put_views(buffers, rb.qdict)

			# Send sentinel all threaded generators
			for q in self.queues.values():
				q.put(None) # type: ignore[arg-type]

			for async_res, generator, t3_unit in zip(self.async_results, self.generators, list(self.queues.keys())):

				# potential T3Record to be included in the T3Document
				if (t3_unit_result := async_res.get()):
					if (z := self.handle_t3_result(t3_unit, t3_unit_result, t3s, generator.stocks, ts)):
						yield z

				self.flush(t3_unit)


	def filter_channels(self, channels: Set[ChannelId]) -> Set[ChannelId]:

		if self.white_list:
			if self.logger.verbose > 1:
				tmp = channels & self.white_list
				for chan in channels - self.white_list:
					if chan not in self.warned:
						self.logger.debug(f"Ignoring channel {chan} (not in white list)")
						self.warned.add(chan)
				channels = tmp
			else:
				channels = channels & self.white_list

		if self.black_list:
			if self.logger.verbose > 1:
				tmp = channels - self.black_list
				for chan in channels & self.black_list:
					if chan not in self.warned:
						self.logger.debug(f"Ignoring channel {chan} (in black list)")
						self.warned.add(chan)
				channels = tmp
			else:
				channels = channels - self.black_list

		return channels
