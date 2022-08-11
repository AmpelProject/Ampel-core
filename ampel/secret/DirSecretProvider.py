import os
import pathlib
from typing import (  # type: ignore[attr-defined]
    Any,
    Dict,
    Type,
    _GenericAlias,
    get_args,
)

import yaml

from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.secret.NamedSecret import NamedSecret
from ampel.secret.Secret import Secret


class DirSecretProvider(AbsSecretProvider):
    """Load secrets from a directory, e.g. a mounted k8s Secret"""

    def __init__(self, path: str) -> None:
        """ """
        self._dir = pathlib.Path(path)
        if not self._dir.is_dir():
            raise NotADirectoryError(path)
        if not os.access(self._dir, os.R_OK):
            raise PermissionError(path)

    def tell(self, arg: Secret, ValueType: Type) -> bool:
        """
        Potentially update an initialized Secret instance with
        the actual sensitive information associable with it.
        :returns: True if the Secret was told/resolved or False
        if the provided Secret is unknown to this secret provider
        """

        if isinstance(arg, NamedSecret):
            try:
                with (self._dir / arg.label).open() as f:
                    value = yaml.safe_load(f)
            except FileNotFoundError:
                return False

            if isinstance(ValueType, _GenericAlias):
                ValueType = get_args(ValueType)  # type: ignore[assignment]

            if isinstance(value, ValueType):
                arg.set(value)
                return True

        return False
