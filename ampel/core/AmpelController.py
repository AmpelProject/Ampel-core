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
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
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
				logger = AmpelLogger.get_logger()
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

			if verbose > 1:
				logger.log(VERBOSE,  # type: ignore[union-attr]
					f'Spawing new process controller with processes: {d[k]}'
				)
			elif verbose == 1:
				logger.debug( # type: ignore[union-attr]
					f'Spawing new process controller with processes: {list(p.name for p in d[k])}'
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
		try:
			return await asyncio.gather(*tasks, return_exceptions=True)
		except asyncio.CancelledError:
			for t in tasks:
				t.cancel()
			return await asyncio.gather(*tasks, return_exceptions=True)

	@staticmethod
	def get_processes(
		config: AmpelConfig,
		tier: Optional[Literal[0, 1, 2, 3]] = None,
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

		for t in ([tier] if tier is not None else [0, 1, 2, 3]): # type: ignore[list-item]
			for p in config.get(f'process.t{t}', dict, raise_exc=True).values():

				# Process name inclusion filter
				if match and not any(rm.match(p['name']) for rm in rmatch):
					if logger:
						if verbose > 1:
							logger.debug('Ignoring process {p["name"]} unmatched by {rmatch}')
						else:
							logger.log(VERBOSE, 'Ignoring unmatched process {p["name"]}')
					continue

				# Process name exclusion filter
				if exclude and any(rx.match(p['name']) for rx in rexcl):
					if logger:
						if verbose > 1:
							logger.info('Excluding process {p["name"]} matched by {rmatch}')
						else:
							logger.info('Excluding matched process {p["name"]}')
					continue

				try:
					# Set defaults
					pm = ProcessModel(**p)
				except Exception as e:
					if logger:
						logger.error(f"Unable to load invalid process {p}", e)
					continue

				# Controller exclusion
				if controllers and pm.controller.unit not in controllers:
					if logger:
						logger.log(VERBOSE, "Ignoring process {p.name} with controller {pm.controller.unit}")
					continue

				ret.append(pm)

		return ret


	@classmethod
	def main(cls):
		from argparse import ArgumentParser
		import signal
		import sys

		from ampel.dev.DictSecretProvider import DictSecretProvider

		parser = ArgumentParser(add_help=True)
		parser.add_argument('config_file_path')
		parser.add_argument('--secrets', type=DictSecretProvider.load, default=None)
		parser.add_argument('--tier', type=int, choices=(0,1,2,3), default=None)
		parser.add_argument('--match', type=str, nargs='*', default=None)
		args = parser.parse_args()

		mcp = cls(**args.__dict__)

		async def shutdown(signal, task, loop):
			"""Stop root task on signal"""
			logging.info(f"Received exit signal {signal.name}...")
			task.cancel()
			await task
			loop.stop()

		loop = asyncio.get_event_loop()
		task = loop.create_task(mcp.run())
		signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
		for s in signals:
			loop.add_signal_handler(
				s,
				lambda s=s: asyncio.create_task(shutdown(s, task, loop))
			)

		for result in loop.run_until_complete(task):
			if isinstance(result, asyncio.CancelledError):
				...
			elif isinstance(result, BaseException):
				raise result
