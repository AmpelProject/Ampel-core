#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/AmpelCoreConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.06.2018
# Last Modified Date: 12.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Literal, Optional, Dict
from ampel.model.EncryptedDataModel import EncryptedDataModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.abstract.AbsAuxiliaryUnit import AbsAuxiliaryUnit


class AmpelCoreConfig(AmpelConfig):
	"""
	Class based on AmpelConfig that provides additional functionalities,
	especially the ability to auto-decrypt encrypted resources
	"""

	def __init__(self, config: Dict, freeze: bool = True) -> None:

		super().__init__(config, freeze)

		# Automatically register auxiliary units
		AbsAuxiliaryUnit.aux.update(
			{
				k: v for t in ('', 't0.unit.', 't1.unit.', 't2.unit.', 't3.unit.')
				for k, v in self.get(f'{t}aux', dict, True).items()
			}
		)


	@staticmethod
	def new_default(
		pwd_file: Optional[str] = None, verbose: bool = False, ignore_errors: bool = False
	) -> 'AmpelConfig':
		"""
		"""
		# Import here to avoid cyclic import error
		from ampel.config.builder.DistConfigBuilder import DistConfigBuilder

		cb = DistConfigBuilder(verbose=verbose)
		cb.load_distributions()

		if pwd_file:
			cb.add_passwords(
				json.load(
					open(pwd_file, "r")
				)
			)

		return AmpelConfig(
			cb.build_config(ignore_errors=ignore_errors)
		)


	def deactivate_all_processes(self) -> None:
		""" """
		for i in range(4):
			self._config[f"t{i}"]['process']['active'] = False


	def activate_process(self,
		channel: str = None, tier: Optional[Literal[0, 1, 2, 3]] = None
	) -> None:
		""" """
		for i in range(4):
			if tier and i != tier:
				continue
			if not channel:
				continue
			if tier == 1:
				self._config[f"t{i}"]['process']['active'] = True
			if tier == 3:
				self._config[f"t{i}"]['process']['active'] = True


	def remove_process(self, tier: Optional[Literal[0, 1, 2, 3]] = None) -> None:
		pass


	def get_resource(self, conf_key: str, debug: bool = False) -> Optional[dict]:
		""" """
		return self.recursive_decrypt(
			f"resource.{conf_key}", debug
		)


	def recursive_decrypt(self, conf_key: str, debug: bool = False) -> Optional[dict]:
		"""
		Note:
		- returns None if conf with provided key is not a Dict
		- returns the fetched dict 'as is' if it is not an encrypted config
		"""
		ret = None
		d = self.get(conf_key)

		if not isinstance(d, dict):
			return ret

		for key in d.keys():

			value = d[key]

			if isinstance(value, dict):

				if "iv" in value:

					if not ret:
						ret = d.copy()

					try:
						ec = EncryptedDataModel(**value)
						ret[key] = ec.decrypt(self.get("pwd"))
					except Exception:
						if debug:
							import traceback
							print("#" * 50)
							traceback.print_exc()
							print("#" * 50)
						self.recursive_decrypt(value)
						continue

		return ret if ret else d
