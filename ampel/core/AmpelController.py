#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/AmpelController.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.04.2020
# Last Modified Date:  14.03.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import asyncio, re
from typing import Literal, TYPE_CHECKING
from collections.abc import Iterable, Sequence

from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.secret.AmpelVault import AmpelVault
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.UnitLoader import UnitLoader
from ampel.log.AmpelLogger import AmpelLogger, DEBUG, VERBOSE
from ampel.util.hash import build_unsafe_dict_id
from ampel.model.ProcessModel import ProcessModel

if TYPE_CHECKING:
	from ampel.protocol.LoggerProtocol import LoggerProtocol


class AmpelController:
	"""
	Top-level controller class that spawns "process controllers" (subclasses of AbsProcessController).
	An ampel config instance or a path to an ampel config is required.
	The process definitions therein (listed under key 'process') are parsed and
	used to spawn the corresponding process controllers.
	The parameters 'tier', 'match', 'exclude' and 'controllers' can be used to sub-select processes.
	"""

	def __init__(
		self,
		config_arg: str | AmpelConfig,
		vault: None | AmpelVault = None,
		tier: None | Literal[0, 1, 2, 3] = None,
		match: None | Sequence[str] = None,
		exclude: None | Sequence[str] = None,
		controllers: None | Sequence[str] = None,
		logger: 'None | LoggerProtocol' = None,
		verbose: int = 0,
		**kwargs,
	):
		"""
		:param config_arg: ampel config or path to ampel config file (json or yaml)
		:param pwd_file_path: if provided, the encrypted conf entries possibly contained \
			in the ampel config instance will be decrypted using the provided password file. \
			The password file must define one password per line.
		:param tier: if specified, only processes defined under the given tier will be scheduled
		:param match: list of regex strings to be matched against process names.
			Only matching processes will be scheduled.
		:param exclude: list of regex strings to be matched against process names.
			Only non-matching processes will be scheduled.
		:param controllers: list of controller class names to be matched against process controller definitions.
			Only processes with matching controller units will be scheduled.
		:param logger: if not provided, a new logger will be instantiated using AmpelLogger.get_logger()
		:param verbose: 1 -> verbose, 2 -> debug
		:param kwargs: will be forwared to the constructor of ampel process controllers
		"""

		self.controllers: list[AbsProcessController] = []
		if isinstance(config_arg, str):
			config = AmpelConfig.load(config_arg, freeze=False)
		else:
			config = config_arg
		loader = UnitLoader(config, db=None, provenance=False, vault=vault)

		if verbose:
			if not logger:
				logger = AmpelLogger.get_logger(
					console={"level": DEBUG if verbose > 1 else VERBOSE}
				)
			logger.log(VERBOSE, "Config file loaded")

		for processes in self.group_processes(
			self.get_processes(
				config,
				tier=tier,
				match=match,
				exclude=exclude,
				controllers=controllers,
				logger=logger,
				verbose=verbose,
			)
		):
			if logger and verbose:
				logger.log(
					VERBOSE,  # type: ignore[union-attr]
					f"Spawing new {processes[0].controller.unit} with processes: {list(p.name for p in processes)}",
				)
			controller_kwargs = {
				"config": config,
				"vault": vault,
				"processes": processes,
				**kwargs,
			}

			self.controllers.append(
				loader.new(
					processes[0].controller,
					unit_type=AbsProcessController,
					**controller_kwargs,
				)
			)


	async def run(self):

		tasks = [
			asyncio.create_task(controller.run())
			for controller in self.controllers
		]
		task = asyncio.gather(*tasks, return_exceptions=True)

		try:
			return await task
		except asyncio.CancelledError:
			for t in tasks:
				t.cancel()
			return await task


	@staticmethod
	def group_processes(processes: list[ProcessModel]) -> list[list[ProcessModel]]:
		"""
		Group processes by controller
		"""
		d: dict[int, list[ProcessModel]] = {}
		for pm in processes:
			controller_id = build_unsafe_dict_id(
				pm.controller.dict(exclude_unset=True), ret=int
			)
			if controller_id in d:
				# Gather process (might raise error in case of invalid process)
				d[controller_id].append(pm)
				continue
			d[controller_id] = [pm]
		return [v for v in d.values()]


	@staticmethod
	def get_processes(
		config: AmpelConfig,
		tier: None | Literal[0, 1, 2, 3, "ops"] = None,
		match: None | Sequence[str] = None,
		exclude: None | Sequence[str] = None,
		controllers: None | Sequence[str] = None,
		logger: 'None | LoggerProtocol' = None,
		verbose: int = 0,
		raise_exc: bool = False,
	) -> list[ProcessModel]:
		"""
		Extract processes from the config. Only active processes are returned.

		:param tier: if specified, only processes defined under a given tier will be returned
		:param match: list of regex strings to be matched against process names.
			Only matching processes will be returned
		:param exclude: list of regex strings to be matched against process names.
			Only non-matching processes will be returned
		:param logger: if provided, information about ignored/excluded processes will be logged
		:param verbose: 1 -> verbose, 2 -> debug
		:param raise_exc: if True, raise ValidationError on invalid processes
		"""

		ret: list[ProcessModel] = []

		if match:
			rmatch = [re.compile(el) for el in match]  # Compile regexes

		if exclude:
			rexcl = [re.compile(el) for el in exclude]

		for t in [tier] if tier is not None else [0, 1, 2, 3, "ops"]:

			tier_name = f"t{t}" if isinstance(t, int) else t
			for p in config.get(f"process.{tier_name}", dict, raise_exc=True).values():

				# Process name inclusion filter
				if match and not any(rm.match(p["name"]) for rm in rmatch):
					if logger:
						if verbose > 1:
							logger.debug(
								f'Ignoring process {p["name"]} unmatched by {rmatch}'
							)
					continue

				# Process name exclusion filter
				if exclude and any(rx.match(p["name"]) for rx in rexcl):
					if logger:
						if verbose > 1:
							logger.info(
								f'Excluding process {p["name"]} matched by {rmatch}'
							)
					continue

				if not p.get("active", True):
					if logger:
						logger.log(VERBOSE, f"Ignoring inactive process {p.get('name')}")
					continue

				try:
					# Set defaults
					pm = ProcessModel(**p)
				except Exception as e:
					if logger:
						logger.error(f"Unable to load invalid process {p}", exc_info=e)
					if raise_exc:
						raise
					continue

				# Controller exclusion
				if controllers and pm.controller.unit not in controllers:
					if logger:
						logger.log(
							VERBOSE,
							f"Ignoring process {pm.name} with controller {pm.controller.unit}",
						)
					continue

				ret.append(pm)

		return ret


	@classmethod
	def main(cls, args: None | list[str] = None) -> None:

		import logging, signal
		from argparse import ArgumentParser
		from ampel.secret.DictSecretProvider import DictSecretProvider

		logging.basicConfig(level="INFO")

		def maybe_int(stringy):
			try:
				return int(stringy)
			except Exception:
				return stringy

		parser = ArgumentParser(description="""Run Ampel processes""")
		parser.add_argument("config_arg", help="Path to Ampel config file")
		parser.add_argument(
			"--secrets",
			default=None,
			help="Path to a YAML secrets store in sops format",
		)
		parser.add_argument(
			"--tier",
			type=maybe_int,
			choices=(0, 1, 2, 3, "ops"),
			default=None,
			help="Run only processes of this tier",
		)
		parser.add_argument(
			"--match",
			type=str,
			nargs="*",
			default=None,
			help="Run only processes whose names match at least one of these regexes",
		)
		parser.add_argument("-v", "--verbose", action="count", default=0)
		args_ns = parser.parse_args(args)
		secrets_file = args_ns.secrets
		if secrets_file is not None:
			args_ns.secrets = DictSecretProvider.load(secrets_file)

		mcp = cls(**args_ns.__dict__)

		def handle_signals(task, loop, graceful=True):
			for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
				loop.remove_signal_handler(s)
				loop.add_signal_handler(
					s,
					lambda s=s, task=task, loop=loop, graceful=graceful: asyncio.create_task(
						shutdown(s, task, loop, graceful)
					),
				)

		async def shutdown(sig, task, loop, graceful=True):
			"""Stop root task on signal"""
			if graceful:
				logging.info(
					f"Received exit signal {sig.name}, shutting down gracefully (signal again to terminate immediately)..."
				)
				for controller in mcp.controllers:
					controller.stop()
				handle_signals(task, loop, False)
			else:
				logging.info(
					f"Received exit signal {sig.name}, terminating immediately..."
				)
				task.cancel()
			await task
			loop.stop()

		def reload_config() -> None:

			try:
				logging.info(f"Reloading config from {args_ns.config_arg}")
				config = AmpelConfig.load(args_ns.config_arg, freeze=False)
				loader = UnitLoader(
					config,
					db=None,
					provenance=False,
					vault=AmpelVault(
						providers=[DictSecretProvider.load(secrets_file)] 
					) if secrets_file else None,
				)
				# Ensure that process models are valid
				with loader.validate_unit_models():
					groups = AmpelController.group_processes(
						AmpelController.get_processes(
							config, tier=args_ns.tier, match=args_ns.match,
						)
					)
			except Exception:
				logging.exception(f"Failed to load {args_ns.config_arg}")
				return

			try:
				controllers = list(mcp.controllers)
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
				assert len(matches) == len(mcp.controllers)
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
		task = loop.create_task(mcp.run())
		handle_signals(task, loop)
		loop.add_signal_handler(signal.SIGUSR1, reload_config)

		for result in loop.run_until_complete(task):
			if isinstance(result, asyncio.CancelledError):
				...
			elif isinstance(result, BaseException):
				raise result
