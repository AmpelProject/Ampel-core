#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/DefaultProcessController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.04.2020
# Last Modified Date: 17.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import asyncio
import schedule
import traceback
import sys
from concurrent import futures
from threading import Lock
from typing import Dict, Sequence, Callable, Any, List, Literal

from ampel.util import concurrent

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelContext import AmpelContext
from ampel.core.Schedulable import Schedulable
from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.ScheduleEvaluator import ScheduleEvaluator
from ampel.model.ProcessModel import ProcessModel
from ampel.model.UnitModel import UnitModel


class DefaultProcessController(AbsProcessController, AmpelBaseModel):
	"""
	Process controller based on ampel.core.Schedulable, i.e which uses
	the module 'schedule' for scheduling ampel processes. It supports:
	- running processes in a dedicated environment using multiprocessing
		if parameter 'isolate' is True in the corresponding ProcessModel
	- running processes in the current environment otherwise
	"""

	def __init__(self,
		config: AmpelConfig,
		processes: Sequence[ProcessModel],
		isolate: bool = False,
		mp_overlap: Literal['terminate', 'wait', 'ignore', 'skip'] = 'skip',
		mp_join: int = 0,
		**kwargs
	) -> None:
		"""
		:param isolate: global isolate override (applies to all processes regardless of \
		the property 'isolate' defined in their own definition. 'isolate' is a process parameter \
		that regulates if a given process should run in its own multiprocessing environment).

		:param mp_overlap: required action if the previously scheduled process did not terminate
		before the next scheduled run is to be executed.
			* terminate: Send SIGTERM to process and join process. It allows processes
			to exit gracefully but can lead to a stall if a process does not behave nicely.
			* kill: Send SIGKILL to process and join process.
			* wait: Join the process. Beware that 'wait' will cause process scheduling to stall.
			* ignore: Run the next scheduled process nonetheless. Warning: this might exhaust resources.
			* skip: Do not run the next scheduled process.
			* skip_once: Do not run the next scheduled process, but remove the currently still active process \
			from the list of registered processes so that the next scheduled process will run for sure next time. \
			Warning: this might exhaust resources

		:param mp_join: test parameter valid only for processes with multiplier == 1, not to be used in production.
		If set to 1, scheduled mp processes will be joined.
		If set to 2, additionaly, the scheduling will be stopped if the processes returns 0/None
		"""

		AbsProcessController.__init__(self, config, processes)
		self.scheduler = schedule.Scheduler()

		if isolate:
			for p in self.proc_models:
				if not p.isolate:
					p.isolate = True

		isolated_processes = [p for p in self.proc_models if p.isolate]

		# one top-level task per ProcessModel
		# invocations of run_async_process
		self._pending_schedules: Set[asyncio.Task] = set()
		# individual process replicas
		self._processes: Dict[str: Set[asyncio.Task]] = dict()

		# Note: no need to freeze config if only isolated processes are to be run since
		# each isolated process will spawn its own AmpelConfig instance in its own environment
		if isolated_processes:

			self.mp_join = mp_join
			self.mp_overlap = mp_overlap

			# get (serializable) ampelconfig content to be provided to mp processes
			# also: lighten the config by removing process definitions which
			# are un-necessary in this case and can be rather large
			if len(isolated_processes) == len(self.proc_models):

				# Having only isolated processes, we can afford to modify the config
				self.mp_config = config.get()
				for el in ('t0', 't1', 't2', 't3'):
					if el in self.mp_config:
						del self.mp_config['process'][el]
			else:

				# Otherwise, do a shallow copy of the first two depths
				# (and remove the 'process' key/values)
				for k, v in config.get().items():
					if k != 'process':
						self.mp_config[k] = {kk: vv for kk, vv in v.items()}

		# If processes are to be run in the current environment, make sure the
		# config is frozen i.e recursively cast members of the loaded config
		# to immutable types (func does nothing if config is already frozen)
		if len(isolated_processes) != len(self.proc_models):
			self.config.freeze()
			self.context = AmpelContext.new(
				tier=self.proc_models[0].tier, config=self.config
			)


	async def run(self) -> None:
		assert self.proc_models
		self.populate_schedule()
		try:
			await (task := asyncio.create_task(self.run_scheduler()))
		except asyncio.CancelledError:
			task.cancel()
			await task


	def populate_schedule(self):
		self.scheduler.clear()
		evaluator = ScheduleEvaluator()
		every = lambda appointment: evaluator(self.scheduler, appointment)
		for pm in self.proc_models:
			for appointment in pm.schedule:
				if not appointment:
					continue
				(
					every(appointment)
					.do(self.run_process, pm=pm)
					.tag(pm.name)
				)


	def run_process(self, pm: ProcessModel):
		if pm.isolate:
			task = asyncio.ensure_future(self.run_async_process(pm))
			self._pending_schedules.add(task)
			task.add_done_callback(lambda t: self._pending_schedules.remove(t))
		else:
			self.run_sync_process(pm)


	async def run_scheduler(self):
		while True:
			try:
				await asyncio.sleep(1)
				self.scheduler.run_pending()
			except asyncio.CancelledError:
				for t in self._pending_schedules:
					t.cancel()
					print(t)
				await asyncio.gather(*self._pending_schedules)
				assert not self._pending_schedules, "all tasks removed themselves"
				raise


	def run_sync_process(self, pm: ProcessModel) -> None:
		""" Runs the provided process in the current environment """

		assert pm.multiplier == 1
		self.context.loader \
			.new_admin_unit(
				unit_model = pm.processor,
				context = self.context,
				sub_type = AbsProcessorUnit,
				log_profile = self.log_profile
			) \
			.run()


	async def run_async_process(self, pm: ProcessModel):
		"""
		Launch target process, potentially after previous invocations have
		finished
		"""
		# gather previously launched processes we might have to wait on
		if not pm.name in self._processes:
			tasks = self._processes[pm.name] = set()
		else:
			tasks = self._processes[pm.name]

		multiplier = pm.multiplier
		if self.mp_overlap in ('terminate', 'wait'):
			# wait until all previous invocations have finished
			to_wait = set(tasks)
			if self.mp_overlap == 'teminate':
				for t in to_wait:
					t.cancel()
			tasks.difference_update(to_wait)
			await asyncio.gather(*to_wait)
		elif self.mp_overlap == 'skip':
			# top up replica set
			tasks.difference_update({t for t in tasks if t.done()})
			multiplier = multiplier - len(tasks)
		elif self.mp_overlap == 'ignore':
			...
		else:
			raise ValueError(f"Unknown overlap type '{self.mp_overlap}'")

		to_wait = {
			self.run_mp_process(
				self.mp_config,
				pm.dict(),
				self.log_profile
			)
			for _ in range(multiplier)
		}
		tasks.update(to_wait)
		try:
			return await asyncio.gather(*to_wait)
		finally:
			tasks.difference_update(to_wait)


	@staticmethod
	@concurrent.process
	def run_mp_process(
		config: Dict[str, Any],
		p: Dict[str, Any],
		log_profile: str = "default"
	) -> Any:

		# Create new context with frozen config
		context = AmpelContext.new(
			tier = p['tier'],
			config = AmpelConfig(config, freeze=True)
		)

		processor = context.loader.new_admin_unit(
			unit_model = UnitModel(**p['processor']),
			context = context,
			sub_type = AbsProcessorUnit,
			log_profile = log_profile
		)

		return processor.run()