#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3DistributiveStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.04.2021
# Last Modified Date: 23.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Optional, List, Generator, Union
from itertools import cycle
from multiprocessing.pool import ThreadPool
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.util.freeze import recursive_freeze
from ampel.model.UnitModel import UnitModel


class T3DistributiveStager(T3BaseStager):
	"""
	Allows to execute a given unit multiple times in different parallel threads (with the same config).
	Each unit processes a subset of the initial ampel buffer stream.
	Example: 2 threads and the buffers ABCD:
	thread1 receives A, thread2 receives B, thread1 receive C, thread2 receives D
	This shall allow better performance when used in combination with T3 units that are slowed down
	by IO based operations (such as network requests to external services).
	Note that no performance gain will be obtained if the processing is CPU limited.
	"""

	#: t3 units (AbsT3Unit) to execute
	execute: UnitModel
	nthread: int = 4

	#: whether to add the thread index into log 'extra' for verbose purposes
	log_extra: bool = False

	#: whether selected stock ids should be saved into the (potential) t3 documents
	save_stock_ids: bool = False


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.t3_units = [
			self.get_unit(self.execute)
			for i in range(self.nthread)
		]


	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Optional[Union[T3Document, List[T3Document]]]:

		try:

			ts = time()
			with ThreadPool(processes=self.nthread) as pool:

				# Create queues and generators for all instanciated t3 units
				queues, generators, async_results = self.create_threaded_generators(pool, self.t3_units)
				View = self.t3_units[0]._View
				qs = queues.values()
				iqs = cycle(qs)

				try:
					for ab in data:
						next(iqs).put(View(**recursive_freeze(ab)))
				except RuntimeError as e:
					if "StopIteration" in str(e):
						return None
					raise e

				# Send sentinel to all threaded generators
				for q in qs:
					q.put(None) # type: ignore[arg-type]

				ret: List[T3Document] = []
				for i, (async_res, generator, t3_unit) in enumerate(zip(async_results, generators, self.t3_units)):

					# potential T3Record to be included in the T3Document
					if (t3_unit_result := async_res.get()):
						if (d := self.handle_t3_result(t3_unit, t3_unit_result, generator.stocks, ts)):
							if self.save_stock_ids:
								d['stock'] = generator.stocks
							ret.append(d)

					self.flush(t3_unit, extra={'thread': i} if self.log_extra else None)

				return ret

		except Exception as e:
			self.flush(self.t3_units)
			self.handle_error(e)

		return None
