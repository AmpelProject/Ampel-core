#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/t1.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.08.2020
# Last Modified Date: 15.08.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.int64 import Int64
from typing import List, Optional
from ampel.type import DataPointId
from ampel.content.T1Document import T1Document
from ampel.log.AmpelLogger import AmpelLogger


def get_datapoint_ids(compound: T1Document, logger: Optional[AmpelLogger] = None) -> List[DataPointId]:

	dps_ids: List[DataPointId] = []

	# Load each datapoint referenced by the loaded compound
	for el in compound['body']:

		# Dict means custom policy/exclusion is set for this datapoint
		if isinstance(el, dict):
			if 'excl' in el:
				if logger:
					logger.debug("Ignoring excluded datapoint", extra={'id': el['id']})
				continue
			dpid = el['id']
		elif isinstance(el, (int, Int64)):
			dpid = el
		else:
			raise StopIteration('Invalid compound doc')

		dps_ids.append(dpid)

	return dps_ids
