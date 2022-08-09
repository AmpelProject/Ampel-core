#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ingest/DualIngestDirective.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.05.2021
# Last Modified Date:  21.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from ampel.types import ChannelId
from ampel.model.ingest.IngestBody import IngestBody
from ampel.model.ingest.FilterModel import FilterModel
from ampel.base.AmpelBaseModel import AmpelBaseModel


class DualIngestDirective(AmpelBaseModel):
	"""
    channel
    filter
    ingest
      new               <- stock was never previously associated with the current channel
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
      known             <- stock was previously associated with the current channel
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

	#: Channel for which to create documents
	channel: ChannelId

	#: Potientially filter input datapoints
	filter: None | FilterModel = None

	ingest: dict[Literal['new', 'known'], IngestBody]
