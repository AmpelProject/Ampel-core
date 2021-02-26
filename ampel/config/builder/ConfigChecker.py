#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigChecker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 25.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import json, os, sys
from typing import Dict, Any, Optional
from ampel.log.AmpelLogger import AmpelLogger, DEBUG, ERROR
from ampel.log.utils import log_exception
from ampel.util.pretty import prettyjson
from ampel.core.AmpelContext import AmpelContext
from ampel.model.UnitModel import UnitModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DictSecretProvider import PotemkinSecretProvider


class ConfigChecker:

	def __init__(self, config: Dict[str, Any], logger: Optional[AmpelLogger] = None, verbose: bool = False):

		self.verbose = verbose
		self.logger = AmpelLogger.get_logger(
			console={'level': DEBUG if verbose else ERROR}
		) if logger is None else logger

		self.ctx = AmpelContext.new(
			config = AmpelConfig(config),
			secrets=PotemkinSecretProvider()
		)

		self.loader = self.ctx.loader

		# Fast deep copy and serialization check
		self.config = json.loads(json.dumps(config))




	def validate(self,
		ignore_inactive: bool = False,
		ignore_ressource_not_avail: bool = True,
		raise_exc: bool = False
	) -> Dict[str, Any]:
		"""
		:returns config if check passed
		:raises: BadConfig
		"""

		self.validate_processor_models(
			ignore_inactive, ignore_ressource_not_avail, raise_exc
		)

		# Recursively load all UnitModels

		# Load all Process models

		return self.config


	def validate_processor_models(self,
		ignore_inactive: bool = False,
		ignore_ressource_not_avail: bool = True,
		raise_exc: bool = False
	) -> None:

		for tier in ("t0", "t1", "t2", "t3"):

			procs = list(self.config['process'][tier].keys())

			for proc in procs:

				if ignore_inactive and not self.config['process'][tier][proc].get('active', True):
					self.logger.info("Ignoring inactivated processor model", extra={"process": proc})
					continue
					
				if self.verbose:
					self.logger.info("Validating processor model", extra={"process": proc})

				try:
					# block potential "Offending value..." print from AmpelBaseModel
					# since we pretty print the input values in case of errors
					sys.stdout = open(os.devnull, 'w')
					self.loader.new_admin_unit(
						UnitModel(
							unit = self.config['process'][tier][proc]['processor']['unit'],
							config = {
								**self.config['process'][tier][proc]['processor']['config'],
								"process_name": proc
							}
						),
						self.ctx
					)
				except Exception as e:

					if ignore_ressource_not_avail and "Global resource not available" in str(e):
						self.logger.error("Ignoring ressource not available error", extra={"process": proc})
					else:

						if raise_exc:
							raise e

						self.logger.error("Processor model validation error", extra={"process": proc})
						for el in prettyjson(self.config['process'][tier][proc]['processor']).split("\n"):
							self.logger.error(el, extra={"process": proc})

						log_exception(self.logger, exc=e, extra={"process": proc})
						del self.config['process'][tier][proc]

				# Restore print
				sys.stdout = sys.__stdout__
