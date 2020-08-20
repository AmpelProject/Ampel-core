#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DictSecretProvider.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.08.2020
# Last Modified Date: 14.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any
import yaml, json

from ampel.abstract.AbsSecretProvider import AbsSecretProvider, Secret


class SecretWrapper(Secret):

    value: Any

    def get(self) -> Any:
        return self.value


class DictSecretProvider(AbsSecretProvider, dict):

    @classmethod
    def load(cls, path) -> 'DictSecretProvider':
        with open(path) as f:
            return cls(yaml.safe_load(f))

    def get(self, key: str) -> SecretWrapper:
        try:
            return SecretWrapper(key=key, value=self[key])
        except KeyError:
            raise KeyError(f"Unknown secret '{key}'")


class PotemkinSecretProvider(AbsSecretProvider):
    def get(self, key: str) -> SecretWrapper:
        return SecretWrapper(key=key, value=key)
