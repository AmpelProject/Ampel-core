#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/IngestBody.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.06.2020
# Last Modified Date: 27.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Sequence, Union
from ampel.model.StrictModel import StrictModel
from ampel.model.ingest.MuxModel import MuxModel
from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T1CombineCompute import T1CombineCompute
from ampel.model.ingest.T1CombineComputeNow import T1CombineComputeNow
from ampel.model.ingest.T2Compute import T2Compute


class IngestBody(StrictModel):
	"""
    stock_t2
    point_t2        <- based on input dps list
    combine         <- same
      state_t2      <- based on dps list returned by combine
      point_t2      <- same
    mux             <- based on input dps list
      insert
        point_t2    <- based on <datapoints insert> result from muxer unit (first list)
      combine
        state_t2    <- based on <datapoints combine> result from muxer unit (second list)
        point_t2    <- same
	"""

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`stocks <ampel.content.StockDocument.StockDocument>`
	stock_t2: Optional[Sequence[T2Compute]]

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`datapoints <ampel.content.DataPoint.DataPoint>`
	#: based on alert content
	point_t2: Optional[Sequence[T2Compute]]

	#: Create :class:`compounds <ampel.content.T1Document.T1Document>` from
	#: combined :class:`datapoints <ampel.content.DataPoint.DataPoint>` and the
	#: associated :class:`T2 documents <ampel.content.T2Document.T2Document>`
	combine: Optional[Sequence[Union[T1Combine, T1CombineCompute, T1CombineComputeNow]]]

	#: Include additional material (such as datapoints from the DB)
	mux: Optional[MuxModel]
