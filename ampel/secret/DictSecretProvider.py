#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/secret/DictSecretProvider.py
# License:             BSD-3-Clause
# Author:              Jakob van Santen <jakob.van.santen@desy.de>
# Date:                14.08.2020
# Last Modified Date:  07.09.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import subprocess, yaml
from typing import Any, get_args, _GenericAlias # type: ignore[attr-defined]
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.secret.Secret import Secret
from ampel.secret.NamedSecret import NamedSecret


class DictSecretProvider(AbsSecretProvider):

	@classmethod
	def load(cls, path: str) -> 'DictSecretProvider':
		"""
		Load from a YAML file. If the file was encrypted with sops_, it will
		be decrypted with ``sops -d``.

		.. _sops: https://github.com/mozilla/sops
		"""
		with open(path) as f:
			payload = yaml.safe_load(f)

		if "sops" in payload:
			try:
				payload = yaml.safe_load(
					subprocess.check_output(['sops', '-d', path])
				)
			except Exception as exc:
				raise RuntimeError(f"Can't read sops-encrypted file {path}") from exc

		return cls(payload)


	def __init__(self, items: dict[str, Any]) -> None:
		""" """
		self.store: dict[str, Any] = dict(items)


	def tell(self, arg: Secret, ValueType: type) -> bool:
		"""
		Potentially update an initialized Secret instance with
		the actual sensitive information associable with it.
		:returns: True if the Secret was told/resolved or False
		if the provided Secret is unknown to this secret provider
		"""

		if isinstance(arg, NamedSecret) and arg.label in self.store:

			if isinstance(ValueType, _GenericAlias):
				ValueType = get_args(ValueType) # type: ignore[assignment]

			if isinstance(value := self.store[arg.label], ValueType):
				arg.set(value)
				return True

		return False
