#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigChecker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 26.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from typing import Any, Dict
from ampel.model.ProcessModel import ProcessModel
from ampel.config.builder.BaseConfigChecker import BaseConfigChecker


class ConfigValidator(BaseConfigChecker):
    """
    Validate a config against the underlying model of process and the
    units defined in their configurations. No unit constructors are called.
    """

    def validate(
        self,
        ignore_inactive: bool = False,
        ignore_ressource_not_avail: bool = True,
        raise_exc: bool = True,
    ) -> Dict[str, Any]:
        """
        :returns: config if check passed
        :raises: BadConfig
        """

        with self.loader.validate_unit_models():
            for tier, proc in self.iter_procs(ignore_inactive):
                ProcessModel(**self.config["process"][tier][proc])

        return self.config
