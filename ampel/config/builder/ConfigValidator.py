#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigValidator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 15.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any
from ampel.base.BadConfig import BadConfig
from ampel.util.pretty import prettyjson
from ampel.model.ProcessModel import ProcessModel
from ampel.config.builder.BaseConfigChecker import BaseConfigChecker


class ConfigValidator(BaseConfigChecker):
    """
    Validate a config against the underlying model of process and the
    units defined in their configurations. No unit constructors are called.
    """

    def validate(self,
        ignore_inactive: bool = False,
        ignore_ressource_not_avail: bool = True,
        raise_exc: bool = True,
    ) -> dict[str, Any]:
        """
        :returns: config if check passed
        :raises: BadConfig
        """

        with self.loader.validate_unit_models():

            for tier, proc in self.iter_procs(ignore_inactive):
                config = self.config["process"][tier][proc]
                try:
                    ProcessModel(**config)
                except Exception as exc:
                    self.logger.debug(f"Invalid process config:\n{prettyjson(config)}")
                    self.logger.error(f"{tier} process {proc} from distribution {config.get('distrib')} is invalid", exc_info=exc)
                    raise BadConfig from exc

        return self.config
