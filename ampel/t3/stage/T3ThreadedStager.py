#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3ThreadedStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.04.2021
# Last Modified Date:  14.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from itertools import islice
from multiprocessing import JoinableQueue
from multiprocessing.pool import ThreadPool, AsyncResult
from collections.abc import Generator, Iterable

from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.view.SnapView import SnapView
from ampel.view.T3Store import T3Store
from ampel.content.T3Document import T3Document
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.t3.stage.ThreadedViewGenerator import ThreadedViewGenerator


class T3ThreadedStager(T3BaseStager, abstract=True):
	"""
	Consumes chunk by chunk the input generator of ampel buffer,
	generates views and updates the view generator of t3 units
	via a JoinableQueue.
	chunk_size determines the max number of views held
	into memory at a given time.

                      chunk size
	                  <------->    chunk #2
	Ampel buffers:    a b c d e |
	Views unit 1:     A B C D E |
	Views unit 2:     A B C D E |
	Ampel buffers:              | f g h i j
	Views unit 1:               | F G H I J
	Views unit 2:               | F G H I J
	                 ---------------------->
	                         time
	"""


	def proceed_threaded(self,
		t3_units: list[AbsT3ReviewUnit],
		buf_gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> Generator[T3Document, None, None]:
		"""
		Execute the method 'process' of t3 units with views crafted using the provided buffer generator and t3 store,
		handle potential results of t3 unit.
		Note: code is not optimized for compactness but for execution speed
		"""

		ts = time()

		try:

			# Create and start T3 units "process(...)" threads (generator will block)
			with ThreadPool(processes=len(t3_units)) as pool:

				queues, generators, async_results = self.create_threaded_generators(pool, t3_units)

				# Optimize by potentially grouping units associated with the same view type
				qdict: dict[type, list[JoinableQueue]] = {}
				for unit in t3_units:
					if unit.__class__._View not in qdict:
						qdict[unit.__class__._View] = []
					qdict[unit.__class__._View].append(queues[unit])

				qv = queues.values()

				try:
				
					while (buffers := list(islice(buf_gen, self.chunk_size)) if self.chunk_size else buf_gen):

						self.put_views(buffers, qdict)

						# Join view queues before possibly processing next chunk
						# to ensure t3 units process views at a similar pace
						for q in qv:
							q.join()

					# Send sentinel to threaded view generators
					for q in qv:
						q.put(None) # type: ignore[arg-type]

					# Collect potential unit results
					for async_res, generator, t3_unit in zip(async_results, generators, t3_units):

						# potential T3Document to be included in the T3Document
						if (t3_unit_result := async_res.get()):
							if (x := self.handle_t3_result(t3_unit, t3_unit_result, t3s, generator.stocks, ts)):
								yield x

				except RuntimeError as e:
					if "StopIteration" in str(e):
						return None
					raise e

			self.flush(t3_units)

		except Exception as e:
			self.flush(t3_units)
			self.event_hdlr.handle_error(e, self.logger)


	def create_threaded_generators(self,
		pool: ThreadPool,
		t3_units: list[AbsT3ReviewUnit],
		t3s: None | T3Store = None
	) -> tuple[
		dict[AbsT3ReviewUnit, "JoinableQueue[SnapView]"],
		list[ThreadedViewGenerator],
		list[AsyncResult]
	]:
		"""
		Create and start T3 units "process(...)" threads (generator will block)
		"""

		queues: dict[AbsT3ReviewUnit, JoinableQueue[SnapView]] = {}
		generators: list[ThreadedViewGenerator] = []
		async_results: list[AsyncResult] = []

		for t3_unit in t3_units:
			queues[t3_unit] = JoinableQueue()
			generators.append(
				ThreadedViewGenerator(
					t3_unit.__class__.__name__,
					queues[t3_unit],
					self.stock_updr
				)
			)
			async_results.append(
				pool.apply_async(
					t3_unit.process,
					args=(generators[-1], t3s)
				)
			)

		return queues, generators, async_results


	def put_views(self, buffers: Iterable[AmpelBuffer], qdict: dict[type[SnapView], list[JoinableQueue]]) -> None:
		"""
		Note: code in here is not optimized for compactness but for execution speed
		"""

		conf = self.context.config

		# Simple case: all t3 units are associated with the same type of view
		if len(qdict) == 1:

			View = next(iter(qdict.keys()))
			qs = next(iter(qdict.values()))
			vo = View.of

			# In paranoia mode, we create a new view from the same buffer for each t3 unit
			if self.paranoia:
				for ab in buffers:
					for q in qs:
						q.put(vo(ab, conf))
			else:
				for ab in buffers:
					v = vo(ab, conf)
					for q in qs:
						q.put(v)

		# t3 units are associated with different type of view
		else:

			# Paranoia or say two units == two view types (non-optimizable)
			itms = qdict.items()
			if self.paranoia or all(len(x) == 1 for x in qdict.values()):
				for ab in buffers:
					for View, qs in itms:
						vo = View.of
						for q in qs:
							q.put(vo(ab, conf))

			# Optimize by potentially grouping units associated with the same view type
			else:
				for ab in buffers:
					for View, qs in itms:
						view = View.of(ab, conf)
						for q in qs:
							q.put(view)
