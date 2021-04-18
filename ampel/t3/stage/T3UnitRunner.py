#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/T3UnitRunner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 07.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Sequence, List, Type, Tuple, TypeVar, get_args, Union

from ampel.type import StockId
from ampel.log import VERBOSE, AmpelLogger, LogFlag
from ampel.log.utils import report_exception
from ampel.log.handlers.ChanRecordBufHandler import ChanRecordBufHandler
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.core.JournalUpdater import JournalUpdater
from ampel.view.SnapView import SnapView
from ampel.content.T3Record import T3Record
from ampel.enum.T3UnitStatus import T3UnitStatus
from ampel.util.mappings import build_unsafe_dict_id
from ampel.util.freeze import recursive_freeze
from ampel.model.t3.T3RunDirective import T3RunDirective
from ampel.struct.JournalTweak import JournalTweak
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
from ampel.t3.run.filter.AbsT3Filter import AbsT3Filter
from ampel.t3.run.project.AbsT3Projector import AbsT3Projector


class RunBlock:
	"""
	Used internally by T3UnitRunner
	"""
	filter: Optional[AbsT3Filter]
	projector: Optional[AbsT3Projector]
	units: List[Tuple[AbsT3Unit, Type]]
	_stock_ids: Optional[List[StockId]]

	def __init__(self):
		self.filter = None
		self.projector = None
		self.units = []
		self._stock_ids = None


class T3UnitRunner(AbsT3UnitRunner):
	"""
	Default implementation handling the instructions defined
	by :class:`T3Directive.run <ampel.model.t3.T3Directive.T3Directive>`.
	"""

	#: Processing specification
	directives: Sequence[T3RunDirective]

	#: whether selected stock ids should be saved into the (potential) t3 documents
	save_stock_ids: bool = True


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.run_blocks: List[RunBlock] = []
		self.units: Dict[int, Tuple[AbsT3Unit, Type[SnapView]]] = {}
		debug = self.logger.verbose > 1

		if debug:
			self.logger.debug("Setting up unit runner")

		for directive in self.directives:

			rb = RunBlock()

			if directive.filter:

				if debug:
					self.logger.debug(f"Setting up filter {directive.filter.unit_name}")

				rb.filter = self.context.loader.new(
					unit_model = directive.filter,
					unit_type = AbsT3Filter,
					logger = self.logger
				)

				if self.save_stock_ids:
					rb._stock_ids = []


			if directive.project:

				if debug:
					self.logger.debug(f"Setting up projector {directive.project.unit_name}")

				rb.projector = AuxUnitRegister.new_unit(
					unit_model = directive.project,
					sub_type = AbsT3Projector,
					logger = self.logger
				)

			if debug:
				self.logger.debug(f"Creating buffering logging handler ({self.channel or 'no channel'})")

			self.buf_hdlr: Union[ChanRecordBufHandler, DefaultRecordBufferingHandler] = \
				ChanRecordBufHandler(self.logger.level, self.channel) if self.channel \
				else DefaultRecordBufferingHandler(self.logger.level)

			for exec_def in directive.execute:

				# Hash config associated with this T3 unit
				config_id = build_unsafe_dict_id(
					{
						"unit": exec_def.unit_name,
						"config": self.context.loader.get_init_config(
							exec_def.unit_name,
							exec_def.config,
							exec_def.override,
							resolve_secrets = False,
						)
					},
					ret = int
				)

				# If a T3 instance exists with the same config, re-use it
				if config_id in self.units:
					rb.units.append(self.units[config_id])
					continue

				if self.logger.verbose:
					if exec_def.config:
						self.logger.log(VERBOSE,
							f"Instantiating unit {exec_def.unit_name} (id: ..{str(config_id)[-6:]})"
						)
					else:
						self.logger.log(VERBOSE, f"Instantiating unit {exec_def.unit_name}")

				# Spawn unit instance
				unit_instance = self.context.loader.new_base_unit(
					unit_model=exec_def,
					logger = AmpelLogger.get_logger(
						base_flag = (getattr(self.logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
						console = False,
						handlers = [self.buf_hdlr]
					),
					sub_type=AbsT3Unit,
					context=self.run_context
				)

				# Get view type from class definition (Generic[T])
				View: Type = get_args(unit_instance.__class__.__orig_bases__[0])[0] # type: ignore
				if type(View) is TypeVar:
					View = SnapView

				# Save unit instance in global unit register
				self.units[config_id] = unit_instance, View

				rb.units.append(self.units[config_id])

			self.run_blocks.append(rb)


	def run(self, data: Sequence[AmpelBuffer]) -> None:

		try:

			jupdater = JournalUpdater(
				ampel_db = self.context.db, tier = 3, run_id = self.run_id,
				process_name = self.process_name, logger = self.logger,
				raise_exc = self.raise_exc, extra_tag = self.extra_journal_tag,
				update_journal = self.update_journal,
			)

			for i, run_block in enumerate(self.run_blocks):

				if self.logger.verbose > 1:
					self.logger.debug(f"Running run-block {i}")

				if run_block.filter:

					if self.logger.verbose:
						self.logger.log(VERBOSE, "Applying run-block filter")

					data = run_block.filter.filter(data)
					run_block._stock_ids.extend([el['id'] for el in data])  # type: ignore[union-attr]

				if run_block.projector:

					if self.logger.verbose:
						self.logger.log(VERBOSE, "Applying run-block projection")

					data = run_block.projector.project(data)

				for t3_unit, View in run_block.units:

					# python: we're all consenting adults
					# ampel: let's rather be suspicious consenting adults
					# We cast ampel buffers into views possibly redundantly (for each sub-unit)
					# since there is no real read-only struct in python

					if self.logger.verbose:
						self.logger.log(VERBOSE, f"Creating views for {t3_unit.__class__.__name__}")

					views = tuple(View(**recursive_freeze(el)) for el in data)

					if ret := t3_unit.add(views):

						unit_name = t3_unit.__class__.__name__

						if isinstance(ret, dict):
							for k, v in ret.items():
								jupdater.add_record(stock=k, jextra=v, unit=unit_name)

						elif isinstance(ret, JournalTweak):
							jupdater.add_record(
								stock = [sv.id for sv in views], jextra = ret, unit = unit_name
							)

						else:
							self.logger.error(
								f"Unsupported result type returned by {unit_name}: {type(ret)}"
							)

					else:
						jupdater.add_record(
							stock = [sv.id for sv in views],
							unit = t3_unit.__class__.__name__
						)

					if self.buf_hdlr.buffer:
						self.buf_hdlr.forward(self.logger)

				if self.update_journal:
					jupdater.flush()

		except Exception as e:

			if self.raise_exc:
				raise e

			# Try to insert doc into trouble collection (raises no exception)
			report_exception(
				self.context.db, self.logger, exc=e,
				info={'process': self.process_name},
			)

		finally:
			if self.buf_hdlr.buffer:
				self.buf_hdlr.forward(self.logger)


	def done(self) -> Optional[Union[T3Record, Sequence[T3Record]]]:
		"""
		:returns: optionally returns T3Record dict(s) to be included in the T3Document
		associated with the current process that will be inserted into the 't3' collection
		"""

		try:

			ret: List[T3Record] = []

			for run_block in self.run_blocks:
				for t3_unit, *_ in run_block.units:

					if (payload := t3_unit.done()):

						# TODO: implement setting version & config
						d = T3Record(
							unit = t3_unit.__class__.__name__,
							status = T3UnitStatus.OK
						)

						if self.channel:
							d['channel'] = self.channel

						if self.save_stock_ids and run_block._stock_ids:
							d['stock'] = run_block._stock_ids

						if isinstance(payload, dict):
							d['payload'] = payload

						ret.append(d)

					if self.buf_hdlr.buffer:
						self.buf_hdlr.forward(self.logger)

			return ret if ret else None

		except Exception as e:

			if self.raise_exc:
				raise e

			# Try to insert doc into trouble collection (raises no exception)
			report_exception(self.context.db, self.logger, exc=e)
			return None

		finally:

			if self.buf_hdlr.buffer:
				self.buf_hdlr.forward(self.logger)
