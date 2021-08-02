#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/IngestDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.05.2021
# Last Modified Date: 20.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.types import ChannelId
from ampel.model.StrictModel import StrictModel
from ampel.model.ingest.IngestBody import IngestBody
from ampel.model.ingest.FilterModel import FilterModel


class IngestDirective(StrictModel):
	"""
    channel
    filter
    ingest
      stock_t2
      point_t2        <- based on input dps list
      combine         <- same
        state_t2      <- based on dps list returned by combine
        point_t2      <- same
      mux             <- based on input dps list
        insert
          point_t2    <- based on <datapoints insert> result from muxer (first list)
        combine
          state_t2    <- based on <datapoints combine> result from muxer (second list)
          point_t2    <- same
	"""

	#: Channel for which to create documents
	channel: ChannelId

	#: Potientially filter input datapoints
	filter: Optional[FilterModel]

	ingest: IngestBody = IngestBody()
