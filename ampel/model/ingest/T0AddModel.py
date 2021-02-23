#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T0AddModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 19.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from pydantic import Field
from typing import List, Optional, Type, Union
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T1CombineModel import T1CombineModel
from ampel.model.ingest.T2ComputeModel import T2ComputeModel
from ampel.abstract.ingest.AbsIngester import AbsIngester

class T0AddModel(UnitModel):
	# Override 'unit' to enable alias
	#: Add :class:`datapoints <ampel.content.DataPoint.DataPoint>` to the database
	unit: Union[str, Type[AbsIngester]] = Field(..., alias='ingester')
	# config (datapoint ingester config [from UnitModel])
	#: Create :class:`compounds <ampel.content.Compound.Compound>` from ingested :class:`datapoints <ampel.content.DataPoint.DataPoint>`
	t1_combine: Optional[List[T1CombineModel]]
	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>` bound to :class:`datapoints <ampel.content.DataPoint.DataPoint>`
	t2_compute: Optional[T2ComputeModel]
