#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/AmpelConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.06.2018
# Last Modified Date: 22.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Dict
from ampel.model.EncryptedData import EncryptedData
from ampel.config.AmpelBaseConfig import AmpelBaseConfig


class AmpelConfig(AmpelBaseConfig):
	""" 
	"""

	@staticmethod
	def new_default(
		pwd_file: str = None, verbose: bool = False, ignore_errors: bool = False
	) -> 'AmpelConfig':
		"""
		"""
		# Import here to avoid cyclic import error
		from ampel.config.builder.DistConfigBuilder import DistConfigBuilder

		cb = DistConfigBuilder(verbose=verbose)
		cb.load_distributions()

		if pwd_file:
			cb.conf_collector.add_passwords(
				json.load(
					open(pwd_file, "r")
				)
			)

		return AmpelConfig(
			cb.get_config(ignore_errors=ignore_errors)
		)


	def deactivate_all_processes(self) -> None:
		""" """
		for i in range(4):
			self._config["t%s"%i]['process']['active'] = False


	def activate_process(self, channel: str = None, tier: int = None) -> None:
		""" """
		for i in range(4):
			if tier and i != tier:
				continue
			if not channel:
				continue
			if tier == 1:
				self._config["t%s"%i]['process']['active'] = True
			if tier == 3:
				self._config["t%s"%i]['process']['active'] = True


	def remove_process(self, tier: int = None) -> None:
		pass


	def recursive_decrypt(self, conf_key: str) -> Dict:
		"""
		Note:
		- returns None if conf with provided key is not a Dict
		- returns the fetched dict 'as is' if it is not an encrypted config
		"""
		ret = None
		d = self.get(conf_key)

		if not isinstance(d, Dict):
			return ret

		for key in d.keys():

			value = d[key]

			if isinstance(value, Dict):

				if "iv" in value:
		
					if not ret:
						ret = d.copy()

					try:
						ec = EncryptedData(**value)
						ret[key] = ec.decrypt(
							self.get("pwd")
						)
					except Exception:
						self.recursive_decrypt(value)
						continue

		return ret if ret else d
