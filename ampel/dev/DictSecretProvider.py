#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DictSecretProvider.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.08.2020
# Last Modified Date: 14.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Tuple
import yaml, json

from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.model.Secret import Secret, T


class SecretWrapper(Secret[T]):

    value: T

    def get(self) -> T:
        return self.value


class DictSecretProvider(AbsSecretProvider, dict):

    @classmethod
    def load(cls, path) -> 'DictSecretProvider':
        with open(path) as f:
            return cls(yaml.safe_load(f))

    def get(self, key: str, type_: T) -> SecretWrapper[T]:
        try:
            return SecretWrapper[type_](key=key, value=self[key])
        except KeyError:
            raise KeyError(f"Unknown secret '{key}'")


class PotemkinSecretProvider(AbsSecretProvider):
    def get(self, key: str, type_: T) -> SecretWrapper[T]:
        origin = getattr(type_, '__origin__', None)
        print({'key': key, 'type': type_, 'origin': origin})
        if origin is tuple:
            value = tuple(t() for t in type_.__args__)
        else:
            value = type_()
        return SecretWrapper[T](key=key, value=value)
