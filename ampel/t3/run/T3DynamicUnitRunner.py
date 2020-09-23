#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/T3DynamicUnitRunnner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 21.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Sequence, Set
from ampel.type import ChannelId
from ampel.model.UnitModel import UnitModel
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.log import AmpelLogger, VERBOSE
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.core import AmpelContext
from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
from ampel.t3.run.T3UnitRunner import T3UnitRunner


class T3DynamicUnitRunner(AbsT3UnitRunner):
	"""
	Unit runner that for each channel found in the elements loaded by the stages:
	
	- spawns a dedicated :class:`~ampel.t3.run.T3UnitRunner.T3UnitRunner` instance configured to filter and project elements wrt this channel
	- execute the associated T3 units

	Example:
	A general T3 process performs a broad, channel-less query.
	Many stocks are returned, each possibly associated with different channels.
	This unit builds a set of all channels referenced by the results.
	Then, for each channel, the information about the referenced elements are posted into
	a dedicated Slack channel whose name corresponds to the Ampel channel. Suppose,
	for example, that the results contain objects:

	- ``<object 1 with channel {A}>``
	- ``<object 2 with channel {A, B}>``
	- ``<object 3 with channel {B, C, D}>``

	This will cause:
	
	- Object 1 and 2 to be posted to slack channel #A
	- Object 2 and 3  to be posted to slack channel #B
	- Object 3 to be posted to slack channel #C
	- Object 3 to be posted to slack channel #D
	"""

	logger: AmpelLogger
	execute: Sequence[UnitModel]
	white_list: Optional[Set[str]] = None
	black_list: Optional[Set[str]] = None

	remove_empty: bool = True
	unalterable: bool = True
	freeze: bool = False


	def __init__(self, context: AmpelContext, **kwargs) -> None:
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

		AmpelBaseModel.__init__(self, **kwargs)
		self.context = context
		self.runners: Dict[ChannelId, T3UnitRunner] = {}

		if self.logger.verbose > 1 and (self.white_list or self.black_list):
			self.warned: Set[ChannelId] = set()

		if self.black_list and self.white_list:
			raise ValueError("Can't have both black and white lists")


	def run(self, data: Sequence[AmpelBuffer]) -> None:

		if not data:
			self.logger.info("No data provided")
			return None

		# step 1: Gather all channels
		# (will raise Error if stock documents were to requested to be loaded)
		channels: Set[ChannelId] = {ell for el in data for ell in el['stock']['channel']}  # type: ignore

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

		elif self.black_list:
			if self.logger.verbose > 1:
				tmp = channels - self.black_list
				for chan in channels & self.black_list:
					if chan not in self.warned:
						self.logger.debug(f"Ignoring channel {chan} (in black list)")
						self.warned.add(chan)
				channels = tmp
			else:
				channels = channels - self.black_list


		# step 2: spawn T3UnitRunner instances
		for chan in channels:

			if chan not in self.runners:

				if self.logger.verbose:
					self.logger.log(VERBOSE, f"Spawning new T3UnitRunner for channel {chan}")

				self.runners[chan] = T3UnitRunner(
					context = self.context,
					logger = self.logger,
					run_id = self.run_id,
					process_name = self.process_name,
					channel = self.channel if self.channel else chan,
					raise_exc = self.raise_exc,
					update_journal = self.update_journal,
					extra_journal_tag = self.extra_journal_tag,
					run_context = self.run_context,
					directives = [
						{
							"filter": {
								"unit": "T3AmpelBufferFilter",
								"config": {"channel": chan}
							},
							"project": {
								"unit": "T3ChannelProjector",
								"config": {"channel": chan, "remove_empty": self.remove_empty}
							},
							"execute": self.execute
						}
					]
				)


		# step 3: run T3UnitRunner instances
		if self.logger.verbose == 1:
			self.logger.log(VERBOSE, "Running unit runners")

		for chan in channels:

			if self.logger.verbose > 1:
				self.logger.debug(f"Running unit runner for chan {chan}")

			self.runners[chan].run(data)

	def done(self) -> None:
		for runner in self.runners.values():
			runner.done()
