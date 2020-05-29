#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/builder/T2UnitIngestionModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2020
# Last Modified Date: 10.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Union, List
from ampel.model.DataUnitModel import DataUnitModel

class T2UnitIngestionModel(DataUnitModel):
	"""
	DataUnitModel defines 'resource'
	"""
	ingest: Optional[Dict[str, Any]]
	group: Union[int, List[int]]
