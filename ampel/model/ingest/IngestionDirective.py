#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/IngestionDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.06.2020
# Last Modified Date: 05.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Union, Optional, List
from ampel.type import ChannelId
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.model.ingest.T0AddModel import T0AddModel
from ampel.model.ingest.T1CombineModel import T1CombineModel
from ampel.model.ingest.T2ComputeModel import T2ComputeModel
from ampel.model.DataUnitModel import DataUnitModel
from ampel.model.AliasedDataUnitModel import AliasedDataUnitModel


class IngestionDirective(AmpelStrictModel):

	channel: ChannelId
	t0_add: T0AddModel
	t1_combine: Optional[List[T1CombineModel]]
	t2_compute: Optional[T2ComputeModel]
	stock_update: Union[DataUnitModel, AliasedDataUnitModel, str]

	@validator('stock_update', pre=True)
	def _allow_str(cls, v):
		if isinstance(v, str):
			return DataUnitModel(unit=v)
		return v
