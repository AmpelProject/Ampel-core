#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/test/dummy.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  11.02.2021
# Last Modified By:    jvs

from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.secret.NamedSecret import NamedSecret


class DummyStateT2Unit(AbsStateT2Unit):

    foo: int = 42
    secret: NamedSecret[int] | None = None

    def process(self, compound, datapoints):
        return {"len": len(datapoints)}
