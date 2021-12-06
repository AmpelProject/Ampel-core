#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/aux/SimpleTagFilter.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 06.12.2021
# Last Modified Date: 06.12.2021
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Optional
from ampel.types import Tag
from ampel.abstract.AbsApplicable import AbsApplicable
from ampel.content.DataPoint import DataPoint


class SimpleTagFilter(AbsApplicable):

    #: Accept DataPoints with any of these tags
    require: Optional[list[Tag]] = None
    #: Reject Datapoints with any of these tags
    forbid: Optional[list[Tag]] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._allow = None if self.require is None else set(self.require)
        self._deny = None if self.forbid is None else set(self.forbid)

    def _accept(self, dp: DataPoint):
        tag = set(dp.get("tag", []))
        return (self._allow is None or tag.intersection(self._allow)) and (
            self._deny is None or not tag.intersection(self._deny)
        )

    def apply(self, arg: list[DataPoint]) -> list[DataPoint]:
        return [el for el in arg if self._accept(el)]
