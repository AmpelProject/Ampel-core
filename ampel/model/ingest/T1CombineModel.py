#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T1CombineModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 19.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from pydantic import Field
from typing import Optional, Type, Union
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T2ComputeModel import T2ComputeModel
from ampel.abstract.ingest.AbsCompoundIngester import AbsCompoundIngester

class T1CombineModel(UnitModel):
	# Override 'unit' to enable alias
	#: Add :class:`compounds <ampel.content.Compound.Compound>` to the database
	unit: Union[str, Type[AbsCompoundIngester]] = Field(..., alias='ingester')
	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>` bound to :class:`compounds <ampel.content.Compound.Compound>`
	t2_compute: Optional[T2ComputeModel]
