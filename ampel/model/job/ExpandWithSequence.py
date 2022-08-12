#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/ExpandWithSequence.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.job.SequenceWithEnd import SequenceWithEnd
from ampel.model.job.SequenceWithCount import SequenceWithCount

class ExpandWithSequence(AmpelBaseModel):

    sequence: SequenceWithCount | SequenceWithEnd

    def items(self):
        for i in self.sequence.items():
            yield self.sequence.format % i

    def __iter__(self):
        return self.items()
