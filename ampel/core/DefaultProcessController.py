#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/DefaultProcessController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.04.2020
# Last Modified Date: 17.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule
from multiprocessing import Queue, Process
from typing import Dict, Sequence, Callable, Any, List, Literal

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelContext import AmpelContext
from ampel.core.Schedulable import Schedulable
from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.ScheduleEvaluator import ScheduleEvaluator
from ampel.model.ProcessModel import ProcessModel
from ampel.model.UnitModel import UnitModel


class DefaultProcessController(AbsProcessController, AmpelBaseModel, Schedulable):
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
		mp_overlap: Literal['terminate', 'kill', 'wait', 'ignore', 'skip', 'skip_once'] = 'terminate',
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
		Schedulable.__init__(self)

		if isolate:
			for p in self.proc_models:
				if not p.isolate:
					p.isolate = True

		isolated_processes = [p for p in self.proc_models if p.isolate]

		# Note: no need to freeze config if only isolated processes are to be run since
		# each isolated process will spawn its own AmpelConfig instance in its own environment
		if isolated_processes:

			self.mp_processes: Dict[str, List[Process]] = {}
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


	def schedule_processes(self) -> None:

		for pm in self.proc_models:
			self.schedule_process(
				pm, DefaultProcessController.create_and_run_mp_process if pm.isolate \
					else self.run_process # type: ignore[arg-type]
			)


	def schedule_process(self, pm: ProcessModel, func: Callable) -> None:

		evaluator = ScheduleEvaluator()
		for appointment in pm.schedule:

			if not appointment:
				continue

			evaluator(self.get_scheduler(), appointment) \
				.do(func, pm=pm) \
				.tag(pm.name)


	def run_process(self, pm: ProcessModel) -> None:
		""" Runs the provided process in the current environment """

		self.context.loader \
			.new_admin_unit(
				unit_model = pm.processor,
				context = self.context,
				sub_type = AbsProcessorUnit,
				verbose = self.verbose
			) \
			.run()


	def create_and_run_mp_process(self, pm: ProcessModel):

		if pm.name in self.mp_processes:

			mpps = self.mp_processes[pm.name]

			# Iterate over copy of list to be able to remove element on the fly
			for mpp in list(mpps):

				if mpp.is_alive():

					if self.mp_overlap == 'kill':
						mpp.kill()
						mpp.join()
						mpps.remove(mpp)
					elif self.mp_overlap == 'terminate':
						mpp.terminate()
						mpp.join()
						mpps.remove(mpp)
					elif self.mp_overlap == 'wait':
						mpp.join()
						mpps.remove(mpp)
					elif self.mp_overlap == 'ignore':
						continue
					elif self.mp_overlap == 'skip':
						return
					elif self.mp_overlap == 'skip_once':
						mpps.remove(mpp)
						if mpps:
							self.create_and_run_mp_process(pm)
						return
					else:
						raise ValueError("Unrecognized mp_overlap value")
				else:
					mpps.remove(mpp)

				mpp.close()

		for i in range(pm.multiplier):

			q: Queue = Queue()
			p: Process = Process(
				target = DefaultProcessController.run_mp_process,
				args = (q, self.mp_config, pm.dict(), self.verbose)
			)

			self.mp_processes[pm.name].append(p)
			p.start()

		if self.mp_join and pm.multiplier == 1:
			self.mp_processes[pm.name][0].join()
			if self.mp_join > 1 and not q.get():
				self.stop()
				return schedule.CancelJob


	@staticmethod
	def run_mp_process(
		queue: Queue,
		config: Dict[str, Any],
		p: Dict[str, Any],
		verbose: int = 0
	) -> None:

		# Create new context with frozen config
		context = AmpelContext.new(
			tier = p['tier'],
			config = AmpelConfig(config, freeze=True)
		)

		processor = context.loader.new_admin_unit(
			unit_model = UnitModel(**p['processor']),
			context = context,
			sub_type = AbsProcessorUnit,
			verbose = verbose
		)

		queue.put(
			processor.run()
		)
