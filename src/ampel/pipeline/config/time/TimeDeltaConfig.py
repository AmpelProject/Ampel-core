#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/time/TimeDeltaConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, constr
from typing import Union, Dict, Any
from ampel.pipeline.common.docstringutils import gendocstring


@gendocstring
class TimeDeltaConfig(BaseModel):
    use: constr(regex='.timeDelta$')
    arguments: Dict[str, Any]
