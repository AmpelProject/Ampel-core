#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/SequenceWithCount.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from ampel.model.job.BaseSequence import BaseSequence

class SequenceWithCount(BaseSequence):

    count: int

    def items(self):
        yield from range(self.start, self.start + self.count)
