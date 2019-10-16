#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeDeltaConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, constr
from typing import Union, Dict, Any
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class TimeDeltaConfig(AmpelBaseModel):
    use: constr(regex='.timeDelta$')
    arguments: Dict[str, Any]
