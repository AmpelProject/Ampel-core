#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T2ComputeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 05.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import Sequence
from ampel.model.ingest.T2IngestModel import T2IngestModel
from ampel.model.UnitModel import UnitModel


class T2ComputeModel(UnitModel):
	# Override of 'unit' to enable alias
	unit: str = Field(..., alias='ingester')
	# config (t2 ingester config [from UnitModel])
	units: Sequence[T2IngestModel]
