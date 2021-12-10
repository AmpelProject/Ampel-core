#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigChecker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 16.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import json, os, sys, traceback
from typing import Any, Dict, List
from ampel.util.pretty import prettyjson
from ampel.util.recursion import walk_and_process_dict
from ampel.core.AmpelContext import AmpelContext
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.model.UnitModel import UnitModel
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.PotemkinSecretProvider import PotemkinSecretProvider
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.config.builder.BaseConfigChecker import BaseConfigChecker


class ConfigChecker(BaseConfigChecker):
	"""
	Validate a config by using it to initialize an AmpelContext, and then
	instantiating all processors and the units defined in their configurations.
	"""

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		self.ctx = AmpelContext.load(
			self.config,
			vault=AmpelVault(providers=[PotemkinSecretProvider()]),
		)


	def validate(self,
		ignore_inactive: bool = False,
		ignore_ressource_not_avail: bool = True,
		raise_exc: bool = False
	) -> Dict[str, Any]:
		"""
		:returns: config if check passed
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
			load_callable(**(model_args | load_args))
	
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
				load_callable = self.loader.new_context_unit,
				load_args = {"context": self.ctx},
				model_args = {
					"model": UnitModel(
						unit=self.config['process'][tier][proc]['processor']['unit'],
						config={
							**self.config['process'][tier][proc]['processor'].get('config', {}),
							"process_name": proc
						}
					),
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
								load_callable = self.loader.new_context_unit,
								load_args = {"context": self.ctx},
								model_args = self._customize_admin_models(um),
								raise_exc = raise_exc,
								ignore_ressource_not_avail = ignore_ressource_not_avail,
							)
						elif um['model']['unit'] in self.config["unit"]["base"]:
							self.load_model(
								tier = tier, proc = proc,
								load_callable = self.loader.new_logical_unit,
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

		if "AbsDocIngester" in self.config["unit"]["admin"][unit_name]["base"]:
			model["config"].update({
				"run_id": 1, "logd": {"logs": [], "extra": {}, "err": False},
				"updates_buffer": DBUpdatesBuffer(self.ctx.db, run_id=1, logger=self.logger)
			})

		elif (
			"AbsT3Stager" in self.config["unit"]["admin"][unit_name]["base"] or
			"AbsT3Supplier" in self.config["unit"]["admin"][unit_name]["base"]
		):
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
