#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DictSecretProvider.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.08.2020
# Last Modified Date: 14.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Dict, Type, Tuple, get_args, get_origin, overload
import subprocess
import yaml

from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.model.Secret import Secret, T


class SecretWrapper(Secret[T]):

    value: T

    def __repr_args__(self):
        return {**dict(super().__repr_args__()), **{'value': '********'}}.items()

    def get(self) -> T:
        return self.value


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
                payload = yaml.safe_load(subprocess.check_output(['sops', '-d', path]))
            except Exception as exc:
                raise RuntimeError(f"Can't read sops-encrypted file {path}") from exc
        return cls(payload)

    def __init__(self, items: Dict[str, Any]) -> None:
        """
        :param items:
        """
        self.store: Dict[str, Any] = dict(items)

    def get(self, key: str, value_type: Type[T]) -> SecretWrapper[T]:
        try:
            value = self.store[key]
        except KeyError:
            raise KeyError(f"Unknown secret '{key}'")
        if origin := get_origin(value_type):
            value_type = origin
        if not isinstance(value, value_type): # type: ignore[arg-type]
            raise ValueError(
                f"Retrieved value has not the expected type.\n"
                f"Expected: {value_type}\n"
                f"Found: {type(value)}"
            )
        return SecretWrapper(key=key, value=value)


class PotemkinSecretProvider(AbsSecretProvider):

    def get(self, key: str, value_type: Type[T]) -> SecretWrapper[T]:
        if get_origin(value_type) is tuple:
            value = tuple(t() for t in get_args(value_type))
        else:
            value = value_type() # type: ignore[assignment]
        return SecretWrapper(key=key, value=value) # type: ignore[arg-type]
