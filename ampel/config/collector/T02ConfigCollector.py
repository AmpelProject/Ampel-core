#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/T02ConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.10.2019
# Last Modified Date: 23.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json
from typing import Dict, Any, Union, List
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import dictify
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class T02ConfigCollector(AbsDictConfigCollector):
	"""
	Collect and hash T2 configurations contained in T0 processes (hence the name T02)
	Note: uses hash algorithm from ampel.util.hash.build_unsafe_dict_id.
	An additional hash collision test is performed using SHA256
	"""

	def add(self,
		arg: Dict[str, Any],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str
	) -> int:

		arg = dictify(arg)

		# Uses xxhash algorithm
		int_hash = build_unsafe_dict_id(arg)

		if self.get(int_hash):

			# Build SHA256 hash
			hash1 = hashlib.sha256(bytes(json.dumps(self.get(int_hash), sort_keys=True), "utf-8")).digest()
			hash2 = hashlib.sha256(bytes(json.dumps(arg, sort_keys=True), "utf-8")).digest()

			if hash1 != hash2:
				raise ValueError("Hash collision detected")

			if self.verbose:
				self.logger.log(VERBOSE, f"Re-using T2 config hash: {int_hash}")

			return int_hash

		if self.verbose:
			self.logger.log(VERBOSE, f"Adding T2 config hash: {int_hash}")

		self.__setitem__(int_hash, arg)
		return int_hash
