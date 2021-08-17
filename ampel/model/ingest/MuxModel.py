#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/MuxModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.05.2021
# Last Modified Date: 27.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Sequence, Literal, Dict, Union
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T1CombineCompute import T1CombineCompute
from ampel.model.ingest.T1CombineComputeNow import T1CombineComputeNow
from ampel.model.ingest.T2Compute import T2Compute


class MuxModel(UnitModel):
	"""
	unit
	model
    insert
      point_t2    <- based on <datapoints insert> result from muxer unit (first list)
    combine
      state_t2    <- based on <datapoints combine> result from muxer unit (second list)
      point_t2    <- same
	"""

	#: Create :class:`compounds <ampel.content.T1Document.T1Document>`
	#: from :class:`datapoints <ampel.content.DataPoint.DataPoint>`
	combine: Optional[Sequence[Union[T1Combine, T1CombineCompute, T1CombineComputeNow]]]

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`datapoints <ampel.content.DataPoint.DataPoint>` based on muxer "insert" result
	insert: Optional[Dict[Literal['point_t2'], Sequence[T2Compute]]]
