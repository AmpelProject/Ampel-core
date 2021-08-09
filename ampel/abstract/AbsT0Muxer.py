#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT0Muxer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.05.2021
# Last Modified Date: 22.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Tuple, Optional
from ampel.types import StockId
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit
from ampel.content.DataPoint import DataPoint
from ampel.log.AmpelLogger import AmpelLogger
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer


class AbsT0Muxer(AmpelABC, ContextUnit, abstract=True):
	"""
	Combines a main source of datapoints with one or more additional sources.
	Unlike t1 combiner, subclass of this unit can fetch and add additional points (possibly from different instruments)
	"""

	# Exclude non-serializable parameters from underlying model by including them as ctor arg
	def __init__(self, logger: AmpelLogger, updates_buffer: DBUpdatesBuffer, **kwargs):
		super().__init__(**kwargs)
		self.logger = logger
		self.updates_buffer = updates_buffer


	@abstractmethod
	def process(self,
		dps: List[DataPoint],
		stock_id: Optional[StockId] = None
	) -> Tuple[Optional[List[DataPoint]], Optional[List[DataPoint]]]:
		"""
		Potentially:
		- Append datapoints to the datapoints provided as argument (the source can be the AmpelDB or external source(s))
		- Reduce to the number of datapoints to insert by determining which datapoints are already in the DB

		Subclasses of this class are used by the ingestion handler,
		which usually performs the following sequence of operation:
		1) shape (T0 unit, must be done first to make sure datapoint ids are avail for comparison)
		2) t0 complete (subclass of this class. For example DB-completition)
		3) ingest
		4) t1 combine

		:returns: Tuple[datapoints to insert, datapoints to combine]
		<datapoints to insert> will be provided to a T0 ingester
		<datapoints to combine> will potentially be provided to an underlying T1 combiner

		Notes regarding <datapoints to combine>:
		- may contain the union between DB datapoints and provided datapoints (dps)
		- it may not contain all datapoints (potiential superseded db datapoints may be excluded)
		- If projections are used by subclass (for optimization purposes),
		  not all the datapoints may contain the same information (some might be missing non-projected keys)
		"""
