#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/time/TimeLastRunConfig.py
# License           : BSD-3-Clause
#gendocstring Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, constr
from ampel.pipeline.common.docstringutils import gendocstring

@gendocstring
class TimeLastRunConfig(BaseModel):
    use: constr(regex='.timeLastRun$')
    jobName: str
