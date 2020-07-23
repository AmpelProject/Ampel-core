#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T2IngestModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 05.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict, List, Union, Optional
from ampel.model.StrictModel import StrictModel

class T2IngestModel(StrictModel):

	unit: str
	config: Optional[int]
	ingest: Optional[Dict[str, Any]]
	group: Union[int, List[int]] = []
