#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigChecker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 26.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import json, os, sys, traceback
from typing import Dict, Any, Optional, Generator, Tuple, List
from ampel.log.AmpelLogger import AmpelLogger, DEBUG, ERROR
from ampel.util.pretty import prettyjson
from ampel.util.mappings import walk_and_process_dict
from ampel.core.AmpelContext import AmpelContext
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.model.UnitModel import UnitModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DictSecretProvider import PotemkinSecretProvider
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer


class ConfigChecker:
	"""
	Validates a config usually build by ConfigBuilder
	"""

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

		# Recursively validate all UnitModels
		self.validate_unit_models(
			ignore_inactive, ignore_ressource_not_avail, raise_exc
		)

		return self.config


	def iter_procs(self,
		ignore_inactive: bool = False,
		raise_exc: bool = False
	) -> Generator[Tuple[str, str], None, None]:

		for tier in ("t0", "t1", "t2", "t3"):

			procs = list(self.config['process'][tier].keys())

			for proc in procs:

				if ignore_inactive and not self.config['process'][tier][proc].get('active', True):
					self.logger.info("Ignoring inactivated processor model", extra={"process": proc})
					continue

				yield (tier, proc)


	def load_model(self,
		tier: str, proc: str, load_callable: Any,
		model_args: Dict[str, Any], raise_exc: bool = False,
		ignore_ressource_not_avail: bool = False,
		load_args: Dict[str, Any] = {}
	) -> bool:
		"""
		:param proc: super process name
		:returns: True on error, False otherwise
		"""

		try:
			# block potential "Offending value..." print from AmpelBaseModel
			# since we pretty print the input values in case of errors
			sys.stdout = open(os.devnull, 'w')
			unit_model = UnitModel(**model_args)
			load_callable(unit_model, **load_args)
	
		except Exception as e:

			if ignore_ressource_not_avail and "Global resource not available" in str(e):
				self.logger.error("Ignoring ressource not available error", extra={"process": proc})
			else:

				if raise_exc:
					raise e

				self.logger.error("Processor model validation error", extra={"process": proc})
				for el in prettyjson(model_args).split("\n"):
					self.logger.error(el, extra={"process": proc})

				self._log_exc(e, proc)
				return True

		finally:
			# Restore print
			sys.stdout = sys.__stdout__

		return False


	def validate_processor_models(self,
		ignore_inactive: bool = False,
		ignore_ressource_not_avail: bool = False,
		raise_exc: bool = False
	) -> None:

		for tier, proc in self.iter_procs(ignore_inactive):

			if self.verbose:
				self.logger.info("Validating processor model", extra={"process": proc})

			if self.load_model(
				tier = tier, proc = proc,
				load_callable = self.loader.new_admin_unit,
				load_args = {"context": self.ctx},
				model_args = {
					"unit": self.config['process'][tier][proc]['processor']['unit'],
					"config": {
						**self.config['process'][tier][proc]['processor'].get('config', {}),
						"process_name": proc
					}
				},
				raise_exc = raise_exc,
				ignore_ressource_not_avail = ignore_ressource_not_avail,
			):
				del self.config['process'][tier][proc]


	def validate_unit_models(self,
		ignore_inactive: bool = False,
		ignore_ressource_not_avail: bool = False,
		raise_exc: bool = False
	) -> None:

		for tier, proc in self.iter_procs(ignore_inactive):

			unit_models: List[Dict[str, Any]] = []
			walk_and_process_dict(
				arg = self.config['process'][tier][proc]['processor'].get('config'),
				callback = self._gather_unit_models_callback,
				match = ['config'],
				unit_models = unit_models
			)

			for um in unit_models:

				try:

					if self.verbose:
						self.logger.debug("Checking model %s" % um['path'])

					if um['model']['unit'] in AuxUnitRegister._defs:
						self.load_model(
							tier = tier, proc = proc,
							load_callable = AuxUnitRegister.new_unit,
							model_args = self._customize_aux_models(um),
							raise_exc = raise_exc,
							ignore_ressource_not_avail = ignore_ressource_not_avail,
						)
					else:
						if um['model']['unit'] in self.config["unit"]["admin"]:
							# Deactivated for now, too complicated
							self.load_model(
								tier = tier, proc = proc,
								load_callable = self.loader.new_admin_unit,
								load_args = {"context": self.ctx},
								model_args = self._customize_admin_models(um),
								raise_exc = raise_exc,
								ignore_ressource_not_avail = ignore_ressource_not_avail,
							)
						elif um['model']['unit'] in self.config["unit"]["base"]:
							self.load_model(
								tier = tier, proc = proc,
								load_callable = self.loader.new_base_unit,
								load_args = {"logger": self.logger},
								model_args = um["model"],
								raise_exc = raise_exc,
								ignore_ressource_not_avail = ignore_ressource_not_avail,
							)

				except Exception as e:

					if raise_exc:
						raise e

					self.logger.error("Unit model validation error", extra={"process": proc})
					for el in prettyjson(um['model']).split("\n"):
						self.logger.error(el, extra={"process": proc})
					self._log_exc(e, proc)


	def _customize_admin_models(self, um: Dict[str, Any]) -> Dict[str, Any]:

		model = json.loads(json.dumps(um['model']))
		unit_name = model['unit']
		if "config" not in model or model["config"] is None:
			model["config"] = {}

		for k in list(model.keys()):
			# Ingester extra keys
			if k in ["units", "t2_compute", "t1_combine", "t3_supervize"]:
				model.pop(k)

		if "AbsIngester" in self.config["unit"]["admin"][unit_name]["base"]:
			model["config"].update({
				"run_id": 1, "logd": {"logs": [], "extra": {}, "err": False},
				"updates_buffer": DBUpdatesBuffer(self.ctx.db, run_id=1, logger=self.logger)
			})

		elif "AbsT3UnitRunner" in self.config["unit"]["admin"][unit_name]["base"]:
			model["config"].update({"process_name": "test", "run_id": 1, "logger": self.logger})

		elif "AbsT3Selector" in self.config["unit"]["admin"][unit_name]["base"]:
			model["config"]["logger"] = self.logger

		return model


	def _customize_aux_models(self, um: Dict[str, Any]) -> Dict[str, Any]:

		model = um['model']
		unit = model['unit']

		if unit in self.config["unit"]["aux"]:
			if "AbsT3Projector" in self.config["unit"]["aux"][unit]["base"]:
				model = json.loads(json.dumps(model))
				if "config" not in model or model["config"] is None:
					model["config"] = {}
				model["config"]["logger"] = self.logger

		return model


	def _gather_unit_models_callback(self, path, k, d, **kwargs) -> None:

		if self.verbose:
			self.logger.debug("# path: %s.config" % path)

		kwargs['unit_models'].append(
			{'path': path, 'model': d}
		)


	def _log_exc(self, e: Exception, proc: str) -> None:
		self.logger.error("=" * 80, extra={"process": proc})
		for el in traceback.format_exception(
			etype=type(e), value=e, tb=e.__traceback__
		):
			for ell in el.split('\n'):
				if len(ell) > 0:
					self.logger.error(ell, extra={"process": proc})
