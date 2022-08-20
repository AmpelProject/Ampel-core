#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/StartCommand.py
# License:             BSD-3-Clause
# Author:              jvs, vb
# Date:                13.03.2021
# Last Modified Date:  20.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import logging, signal, asyncio
from argparse import ArgumentParser
from typing import Any
from collections.abc import Sequence
from ampel.abstract.AbsCLIOperation import AbsCLIOperation
from ampel.core.AmpelController import AmpelController
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
#from ampel.log.AmpelLogger import AmpelLogger
#from ampel.log.LogFlag import LogFlag
from ampel.secret.AmpelVault import AmpelVault
from ampel.config.AmpelConfig import AmpelConfig


# Help parameter descriptions
h = {
	"config": "Path to an ampel config file (yaml/json)",
	"secrets": "Path to a YAML secrets store in sops format",
	"process": "Regex matching a process name. --process can be specified multiple times",
	"tier": "Run only processes of this tier",
	"exclude": "Specifically ecclude process(es) using this regex",
	"skip-validation": "Skip validation of unit models (safe to use if the ampel conf is trusted)",
	"verbose": "Increases verbosity"
}

class ScheduleCommand(AbsCLIOperation):
	"""
	TODO: implement properly
	"""

	@staticmethod
	def get_sub_ops() -> None | list[str]:
		return None

	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		parser = AmpelArgumentParser()
		parser.set_help_descr(h)

		parser.req("config")
		parser.req("process", type=str, nargs="*", default=None)

		parser.opt("exclude", type=str, nargs="*", default=None)
		parser.opt("tier", action=MaybeIntAction, choices=(0, 1, 2, 3, "ops"), default=None)
		parser.opt("secrets", default=None)
		parser.opt("skip-validation", action='store_true')
		parser.opt("verbose", action="count", default=0)

		return parser


	# Mandatory implementation
	def run(self,
		args: dict[str, Any],
		unknown_args: Sequence[str],
		sub_op: None | str = None
	) -> None:

		self.args = args
		self.unknown_args = unknown_args
		# logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)
		config = AmpelConfig.load(args['config'])

		if (secrets_file := args['secrets']):
			from ampel.secret.DictSecretProvider import DictSecretProvider
			secrets = DictSecretProvider.load(secrets_file)

		self.el_capitan = AmpelController(
			config_arg = config,
			vault = AmpelVault(providers=[secrets]) if args['secrets'] else None,
			tier = args['tier'] if args['tier'] else None,
			match = args['process'],
			exclude = args['exclude'] if args['exclude'] else None,
			verbose = args['verbose']
		)

		if not self.el_capitan.controllers:
			print("No matching process found")
			return

		loop = asyncio.get_event_loop()
		task = loop.create_task(self.el_capitan.run())
		self.handle_signals(task, loop)
		loop.add_signal_handler(signal.SIGUSR1, self.reload_config)

		for result in loop.run_until_complete(task):
			if isinstance(result, asyncio.CancelledError):
				...
			elif isinstance(result, BaseException):
				raise result


	def handle_signals(self, task, loop, graceful=True):

		for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
			loop.remove_signal_handler(s)
			loop.add_signal_handler(
				s,
				lambda s=s, task=task, loop=loop, graceful=graceful: asyncio.create_task(
					self.shutdown(s, task, loop, graceful)
				),
			)


	async def shutdown(self, sig, task, loop, graceful=True):
		"""Stop root task on signal"""

		if graceful:
			logging.info(
				f"Received exit signal {sig.name}, shutting down gracefully (signal again to terminate immediately)..."
			)
			for controller in self.el_capitan.controllers:
				controller.stop()
			self.handle_signals(task, loop, False)
		else:
			logging.info(
				f"Received exit signal {sig.name}, terminating immediately..."
			)
			task.cancel()

		await task
		loop.stop()


	def reload_config(self) -> None:

		if self.args is None:
			return

		try:

			logging.info(f"Reloading config from {self.args['config']}")
			config = AmpelConfig.load(self.args['config'])

			secrets = None
			if self.args['secrets']:
				from ampel.secret.DictSecretProvider import DictSecretProvider
				secrets = DictSecretProvider.load(self.args['secrets'])

			if self.args['skip_validation']:
				import contextlib
				pyctx: Any = contextlib.nullcontext
			else:
				from ampel.core.UnitLoader import UnitLoader
				loader = UnitLoader(
					config,
					db=None,
					provenance=False,
					vault=AmpelVault(providers=[secrets]) if secrets else None
				)
				pyctx = loader.validate_unit_models

			with pyctx():
				groups = AmpelController.group_processes(
					AmpelController.get_processes(
						config, tier=self.args['tier'], match=self.args['match'],
					)
				)

		except Exception:
			logging.exception(f"Failed to load {self.args['config']}")
			return

		try:
			controllers = list(self.el_capitan.controllers)
			matches = []

			for group in groups:
				names = {pm.name for pm in group}
				for i, candidate in enumerate(controllers):
					if names.intersection([pm.name for pm in candidate.processes]):
						matches.append((candidate, group))
						del controllers[i]
						break
				else:
					raise RuntimeError(f"No match for process group {names}")
			assert len(matches) == len(self.el_capitan.controllers)

		except Exception:
			logging.exception("Failed to match process groups with current set")

		for controller, processes in matches:
			try:
				controller.update(config, loader.vault, processes)
				logging.info(
					f"Updated {controller.__class__.__name__} with processes: {[pm.name for pm in processes]} "
				)
			except Exception:
				logging.exception(
					f"Failed to update {controller.__class__.__name__} with processes: {[pm.name for pm in processes]}"
				)

		loop = asyncio.get_event_loop()
		task = loop.create_task(self.el_capitan.run())
		self.handle_signals(task, loop)
		loop.add_signal_handler(signal.SIGUSR1, self.reload_config)

		for result in loop.run_until_complete(task):
			if isinstance(result, asyncio.CancelledError):
				...
			elif isinstance(result, BaseException):
				raise result
