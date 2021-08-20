#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/DefaultProcessController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.04.2020
# Last Modified Date: 17.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import asyncio, datetime, logging, schedule
from functools import partial
from typing import Dict, Sequence, Any, Literal, Optional, Set

from ampel.util import concurrent
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.core.AmpelContext import AmpelContext
from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.secret.AmpelVault import AmpelVault
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.ScheduleEvaluator import ScheduleEvaluator
from ampel.model.ProcessModel import ProcessModel
from ampel.core.AmpelDB import AmpelDB
from ampel.core.UnitLoader import UnitLoader

log = logging.getLogger(__name__)


class DefaultProcessController(AbsProcessController):
	"""
	Process controller based on ampel.core.Schedulable, i.e which uses
	the module 'schedule' for scheduling ampel processes. It supports:
	- running processes in a dedicated environment using multiprocessing
		if parameter 'isolate' is True in the corresponding ProcessModel
	- running processes in the current environment otherwise
	"""

	isolate: bool = False
	mp_overlap: Literal['terminate', 'wait', 'ignore', 'skip'] = 'skip'
	mp_join: int = 0


	def __init__(self, **kwargs) -> None:
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

		super().__init__(**kwargs)
		self.scheduler = schedule.Scheduler()

		# one top-level task per ProcessModel
		# invocations of run_async_process
		self._pending_schedules: Set[asyncio.Future] = set()
		# individual process replicas
		self._processes: Dict[str, Set[asyncio.Task]] = dict()

		self._prepare_isolated_processes()


	def _prepare_isolated_processes(self) -> None:

		if self.isolate:
			for p in self.processes:
				if not p.isolate:
					p.isolate = True

		isolated_processes = [p for p in self.processes if p.isolate]

		# Note: no need to freeze config if only isolated processes are to be run since
		# each isolated process will spawn its own AmpelConfig instance in its own environment
		if isolated_processes:

			# get (serializable) ampelconfig content to be provided to mp processes
			# also: lighten the config by removing process definitions which
			# are un-necessary in this case and can be rather large
			if len(isolated_processes) == len(self.processes):

				# Having only isolated processes, we can afford to modify the config
				self.mp_config = self.config.get()
				for el in ('t0', 't1', 't2', 't3'):
					if el in self.mp_config:
						del self.mp_config['process'][el]
			else:

				# Otherwise, do a shallow copy of the first two depths
				# (and remove the 'process' key/values)
				for k, v in self.config.get().items():
					if k != 'process':
						self.mp_config[k] = {kk: vv for kk, vv in v.items()}

		# If processes are to be run in the current environment, make sure the
		# config is frozen i.e recursively cast members of the loaded config
		# to immutable types (func does nothing if config is already frozen)
		if len(isolated_processes) != len(self.processes):
			self.config.freeze()
			db = AmpelDB.new(self.config, self.vault)
			self.context = AmpelContext(
				self.config, db=db, loader=UnitLoader(self.config, db=db, vault=self.vault),
			)


	def update(self,
		config: AmpelConfig,
		vault: Optional[AmpelVault],
		processes: Sequence[ProcessModel]
	) -> None:
		self.config = config
		self.processes = processes
		self.vault = vault
		self._prepare_isolated_processes()
		self.populate_schedule(now=False)


	async def run(self) -> None:

		assert self.processes
		self.populate_schedule(now=True)
		try:
			await (task := asyncio.create_task(self.run_scheduler()))
		except asyncio.CancelledError:
			task.cancel()
			await task


	def stop(self, name: Optional[str] = None) -> None:
		"""Stop scheduling new processes."""
		self.scheduler.clear(tag=name)


	def populate_schedule(self, now: bool) -> None:
		"""
		Prepare task schedule
		
		:param now: schedule unanchored tasks (i.e. those with no at_time) now
			rather than one period from now
		"""
		self.scheduler.clear()
		evaluator = ScheduleEvaluator()
		every = lambda appointment: evaluator(self.scheduler, appointment)
		for pm in self.processes:
			for appointment in pm.schedule:
				if not appointment:
					continue
				if appointment == "super":
					from ampel.log.AmpelLogger import AmpelLogger
					AmpelLogger.get_logger().error("DefaultProcessController does not handle the schedule value 'super'")
					continue
				job = (
					every(appointment)
					.do(self.run_process, pm=pm)
					.tag(pm.name)
				)
				# Pull back the first run if the first wait time is within 10
				# seconds of the period
				if now and abs((job.next_run-datetime.datetime.now()-job.period).total_seconds()) < 10:
					job.next_run -= job.period


	def _finalize_task(self, pm: ProcessModel, future: asyncio.Future) -> None:

		try:
			result = future.result()
			if self.mp_join >= 2 and not any(bool(r) for r in result):
				for job in [job for job in self.scheduler.jobs if pm.name in job.tags]:
					self.scheduler.cancel_job(job)
		except asyncio.CancelledError:
			return
		except Exception as exc:
			AbsProcessController.process_exceptions.labels(pm.tier, pm.name).inc()
			log.warn(f"{pm.name} failed", exc_info=exc)
		finally:
			self._pending_schedules.remove(future)


	def run_process(self, pm: ProcessModel) -> None:

		if pm.isolate:
			task = asyncio.ensure_future(self.run_async_process(pm))
			self._pending_schedules.add(task)
			task.add_done_callback(partial(self._finalize_task, pm))
		else:
			self.run_sync_process(pm)


	async def run_scheduler(self) -> None:

		try:
			while self.scheduler.jobs:
				try:
					if self.mp_join >= 1 and self._pending_schedules:
						await asyncio.gather(*self._pending_schedules)
					if self.mp_join >= 2 and not self.scheduler.jobs:
						break
					await asyncio.sleep(1)
					self.scheduler.run_pending()
				except asyncio.CancelledError:
					for t in self._pending_schedules:
						t.cancel()
					raise
		finally:
			# We land here when the loop breaks due to a call to stop() or
			# this coroutine is cancelled. In either case, wait for any
			# in-flight processes to exit.
			await asyncio.gather(*self._pending_schedules)
			assert not self._pending_schedules, "all tasks removed themselves"


	def run_sync_process(self, pm: ProcessModel) -> None:
		""" Runs the provided process in the current environment """

		assert pm.multiplier == 1
		self.context.loader \
			.new_context_unit(
				model = pm.processor,
				context = self.context,
				sub_type = AbsEventUnit,
			) \
			.run()


	async def run_async_process(self, pm: ProcessModel) -> Sequence:
		"""
		Launch target process, potentially after previous invocations have
		finished
		"""
		# gather previously launched processes we might have to wait on
		if pm.name not in self._processes:
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

		def launch():
			counter = self.process_count.labels(pm.tier, pm.name)
			t = self.run_mp_process(
				self.mp_config,
				self.vault,
				pm.dict(),
			)
			counter.inc()
			t.add_done_callback(lambda t: counter.dec())
			return t

		to_wait = {launch() for _ in range(multiplier)}
		tasks.update(to_wait)
		try:
			return await asyncio.gather(*to_wait)
		finally:
			tasks.difference_update(to_wait)


	@staticmethod
	@concurrent.process
	def run_mp_process(
		config: Dict[str, Any],
		vault: Optional[AmpelVault],
		p: Dict[str, Any],
	) -> Any:

		pm = ProcessModel(**p)

		try:
			import setproctitle
			setproctitle.setproctitle(f"ampel.t{pm.tier}.{pm.name}")
		except Exception:
			...

		# Create new context with frozen config
		context = AmpelContext.load(
			config,
			vault=vault,
			freeze_config=True
		)

		processor = context.loader.new_context_unit(
			model = pm.processor,
			context = context,
			sub_type = AbsEventUnit,
			process_name = pm.name,
		)

		return processor.run()
