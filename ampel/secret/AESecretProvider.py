#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/secret/AEAbsSecretProvider.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.06.2021
# Last Modified Date:  20.06.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from sjcl import SJCL
from typing import Union
from collections.abc import Iterable
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.secret.AESecret import AESecret
from ampel.secret.Secret import Secret


class AESecretProvider(AbsSecretProvider):

	def __init__(self, pwds: str | Iterable[str]):
		self.sjcl = SJCL()
		self.pwds = [pwds] if isinstance(pwds, str) else pwds


	def tell(self, arg: Secret, ValueType: type) -> bool:
		"""
		Potentially update an initialized Secret instance with
		the actual sensitive information associable with it.
		:returns: True if the Secret was told/resolved or
		False if the provided Secret is unknown to this secret provider
		"""

		if not issubclass(str, ValueType):
			return False

		if isinstance(arg, AESecret):
			for pwd in self.pwds:
				try:
					arg.set(
						self.sjcl \
							.decrypt(arg.dict(), pwd) \
							.decode("utf-8")
					)
					return True
				except Exception:
					continue

		return False
