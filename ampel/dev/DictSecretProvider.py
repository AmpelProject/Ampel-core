#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DictSecretProvider.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.08.2020
# Last Modified Date: 14.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Dict, get_args, get_origin
import yaml

from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.model.Secret import Secret, T


class SecretWrapper(Secret[T]):

    value: T

    def get(self) -> T:
        return self.value


class DictSecretProvider(AbsSecretProvider):

    @classmethod
    def load(cls, path) -> 'DictSecretProvider':
        with open(path) as f:
            return cls(yaml.safe_load(f))

    def __init__(self, dictlike: Dict[str,Any]) -> None:
        self.store : Dict[str,Any] = dict(dictlike)

    # FIXME: find a way to express the desired return type statically
    def get(self, key: str, type_: T) -> SecretWrapper[T]:
        try:
            return SecretWrapper[T](key=key, value=self.store[key])
        except KeyError:
            raise KeyError(f"Unknown secret '{key}'")


class PotemkinSecretProvider(AbsSecretProvider):
    def get(self, key: str, type_: T) -> SecretWrapper[T]:
        if get_origin(type_) is tuple:
            value = tuple(t() for t in get_args(type_))
        else:
            value = type_() # type: ignore[operator]
        return SecretWrapper[T](key=key, value=value) # type: ignore[arg-type]
