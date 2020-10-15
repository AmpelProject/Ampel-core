#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.04.2020
# Last Modified Date: 17.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib, re
from typing import Dict, Sequence, List, Literal, Optional, Iterable
import asyncio
import logging
import yaml

from ampel.core.Schedulable import Schedulable
from ampel.config.AmpelConfig import AmpelConfig
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE, DEBUG
from ampel.model.ProcessModel import ProcessModel
from ampel.util.mappings import build_unsafe_dict_id
from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.core.UnitLoader import UnitLoader

class AmpelController:
	"""
	Top-level controller class whose purpose is the spawn "process controllers"
	(i.e subclasses of AbsProcessController).
	This can be done scoped at a given tier or generally for all ampel tiers.
	Processes can be filtered out/included via regular expression matching (the process names).
	"""

	def __init__(self,
		config_file_path: str,
		pwd_file_path: Optional[str] = None,
		pwds: Optional[Iterable[str]] = None,
		secrets: Optional[AbsSecretProvider] = None,
		tier: Optional[Literal[0, 1, 2, 3]] = None,
		match: Optional[Sequence[str]] = None,
		exclude: Optional[Sequence[str]] = None,
		controllers: Optional[Sequence[str]] = None,
		logger: Optional[AmpelLogger] = None,
		verbose: int = 0,
		**kwargs
	):
		"""
		:param config_file_path: path to the central ampel config file (json or yaml)
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


		d: Dict[int, List[ProcessModel]] = {}
		self.controllers: List[AbsProcessController] = []
		config = AmpelConfig.load(config_file_path, pwd_file_path, pwds, freeze=False)
		loader = UnitLoader(config, secrets=secrets)

		if verbose:
			if not logger:
				logger = AmpelLogger.get_logger(console={"level": DEBUG if verbose > 1 else VERBOSE})
			logger.log(VERBOSE, "Config file loaded")

		for pm in self.get_processes(
			config, tier=tier, match=match, exclude=exclude,
			controllers=controllers, logger=logger, verbose=verbose
		):
			controller_id = build_unsafe_dict_id(pm.controller.dict(exclude_none=True), ret=int)
			if controller_id in d:
				# Gather process (might raise error in case of invalid process)
				d[controller_id].append(pm)
				continue

			d[controller_id] = [pm]

		for k, processes in d.items():

			if verbose:
				logger.log(VERBOSE,  # type: ignore[union-attr]
					f'Spawing new {processes[0].controller.unit} with processes: {list(p.name for p in d[k])}'
				)
			controller_kwargs = {
				'config': config,
				'secrets': loader.secrets,
				'processes': processes,
				**kwargs
			}

			self.controllers.append(
				loader.new(processes[0].controller, unit_type=AbsProcessController, **controller_kwargs)
			)


	async def run(self):
		tasks = [asyncio.create_task(controller.run()) for controller in self.controllers]
		task = asyncio.gather(*tasks, return_exceptions=True)
		try:
			return await task
		except asyncio.CancelledError:
			for t in tasks:
				t.cancel()
			return await task

	@staticmethod
	def get_processes(
		config: AmpelConfig,
		tier: Optional[Literal[0, 1, 2, 3, "ops"]] = None,
		match: Optional[Sequence[str]] = None,
		exclude: Optional[Sequence[str]] = None,
		controllers: Optional[Sequence[str]] = None,
		logger: Optional[AmpelLogger] = None,
		verbose: int = 0
	) -> List[ProcessModel]:
		"""
		:param tier: if specified, only processes defined under a given tier will be returned
		:param match: list of regex strings to be matched against process names.
			Only matching processes will be returned
		:param exclude: list of regex strings to be matched against process names.
			Only non-matching processes will be returned
		:param logger: if provided, information about ignored/excluded processes will be logged
		:param verbose: 1 -> verbose, 2 -> debug
		"""

		ret: List[ProcessModel] = []

		if match:
			rmatch = [re.compile(el) for el in match] # Compile regexes

		if exclude:
			rexcl = [re.compile(el) for el in exclude]

		for t in ([tier] if tier is not None else [0, 1, 2, 3, "ops"]): # type: ignore[list-item]
			tier_name = f"t{t}" if isinstance(t, int) else t
			for p in config.get(f'process.{tier_name}', dict, raise_exc=True).values():

				# Process name inclusion filter
				if match and not any(rm.match(p['name']) for rm in rmatch):
					if logger:
						if verbose > 1:
							logger.debug(f'Ignoring process {p["name"]} unmatched by {rmatch}')
					continue

				# Process name exclusion filter
				if exclude and any(rx.match(p['name']) for rx in rexcl):
					if logger:
						if verbose > 1:
							logger.info(f'Excluding process {p["name"]} matched by {rmatch}')
					continue

				try:
					# Set defaults
					pm = ProcessModel(**p)
				except Exception as e:
					if logger:
						logger.error(f"Unable to load invalid process {p}", e)
					continue

				if not pm.active:
					if logger:
						logger.log(VERBOSE, f"Ignoring inactive process {p.name}")
					continue

				# Controller exclusion
				if controllers and pm.controller.unit not in controllers:
					if logger:
						logger.log(VERBOSE, f"Ignoring process {p.name} with controller {pm.controller.unit}")
					continue

				ret.append(pm)

		return ret


	@classmethod
	def main(cls, args: Optional[List[str]]=None) -> None:
		from argparse import ArgumentParser
		import logging
		import signal
		import sys

		logging.basicConfig(level='INFO')

		from ampel.dev.DictSecretProvider import DictSecretProvider

		def maybe_int(stringy):
			try:
				return int(stringy)
			except:
				return stringy

		parser = ArgumentParser(add_help=True)
		parser.add_argument('config_file_path')
		parser.add_argument('--secrets', type=DictSecretProvider.load, default=None)
		parser.add_argument('--tier', type=maybe_int, choices=(0,1,2,3,"ops"), default=None)
		parser.add_argument('--match', type=str, nargs='*', default=None)
		parser.add_argument('-v', '--verbose', action='count', default=0)
		args = parser.parse_args(args)

		mcp = cls(**args.__dict__)

		def handle_signals(task, loop, graceful=True):
			for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
				loop.remove_signal_handler(s)
				loop.add_signal_handler(
					s,
					lambda s=s, task=task, loop=loop, graceful=graceful: asyncio.create_task(shutdown(s, task, loop, graceful))
				)

		async def shutdown(sig, task, loop, graceful=True):
			"""Stop root task on signal"""
			if graceful:
				logging.info(f"Received exit signal {sig.name}, shutting down gracefully (signal again to terminate immediately)...")
				for controller in mcp.controllers:
					controller.stop()
				handle_signals(task, loop, False)
			else:
				logging.info(f"Received exit signal {sig.name}, terminating immediately...")
				task.cancel()
			await task
			loop.stop()

		loop = asyncio.get_event_loop()
		task = loop.create_task(mcp.run())
		handle_signals(task, loop)

		for result in loop.run_until_complete(task):
			if isinstance(result, asyncio.CancelledError):
				...
			elif isinstance(result, BaseException):
				raise result
