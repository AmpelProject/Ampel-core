#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T2ComputeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 19.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from pydantic import Field
from typing import Sequence, Union, Type
from ampel.model.ingest.T2IngestModel import T2IngestModel
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester
from ampel.model.UnitModel import UnitModel


class T2ComputeModel(UnitModel):
	# Override of 'unit' to enable alias
	#: Add :class:`T2 documents <ampel.content.T2Document.T2Document>` to the database
	unit: Union[str, Type[AbsT2Ingester]] = Field(..., alias='ingester')
	# config (t2 ingester config [from UnitModel])
	#: Specification of :class:`T2 documents <ampel.content.T2Document.T2Document>` to create
	units: Sequence[T2IngestModel]
