#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3DistributiveStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                22.04.2021
# Last Modified Date:  09.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from itertools import cycle
from collections.abc import Generator
from multiprocessing.pool import ThreadPool

from ampel.view.T3Store import T3Store
from ampel.model.UnitModel import UnitModel
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.t3.stage.T3ThreadedStager import T3ThreadedStager


class T3DistributiveStager(T3ThreadedStager):
	"""
	Allows to execute a given unit multiple times in different parallel threads (with the same config).
	Each unit processes a subset of the initial ampel buffer stream.
	Example: 2 threads and the buffers ABCD:
	thread1 receives A, thread2 receives B, thread1 receive C, thread2 receives D
	This shall allow better performance when used in combination with T3 units that are slowed down
	by IO based operations (such as network requests to external services).
	Note that no performance gain will be obtained if the processing is CPU limited.
	"""

	#: t3 units (AbsT3ReviewUnit) to execute
	execute: UnitModel
	nthread: int = 4

	#: whether to add the thread index into log 'extra' for verbose purposes
	log_extra: bool = False


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.t3_units = [
			self.get_unit(self.execute)
			for i in range(self.nthread)
		]


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:

		try:

			ts = time()
			with ThreadPool(processes=self.nthread) as pool:

				# Create queues and generators for all instanciated t3 units
				queues, generators, async_results = self.create_threaded_generators(pool, self.t3_units, t3s)
				View = self.t3_units[0]._View
				qs = queues.values()
				iqs = cycle(qs)

				try:
					for ab in gen:
						next(iqs).put(View.of(ab, self.context.config, freeze=True))
				except RuntimeError as e:
					if "StopIteration" in str(e):
						return None
					raise e

				# Send sentinel to all threaded generators
				for q in qs:
					q.put(None) # type: ignore[arg-type]

				for i, (async_res, generator, t3_unit) in enumerate(zip(async_results, generators, self.t3_units)):

					# potential T3Record to be included in the T3Document
					if (t3_unit_result := async_res.get()):
						if (d := self.handle_t3_result(t3_unit, t3_unit_result, t3s, generator.stocks, ts)):
							if self.save_stock_ids:
								d['stock'] = generator.stocks
							yield d

					self.flush(t3_unit, extra={'thread': i} if self.log_extra else None)

		except Exception as e:
			self.flush(self.t3_units)
			self.event_hdlr.handle_error(e, self.logger)
