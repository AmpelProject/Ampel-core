#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsStockSelector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 09.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Dict, Any, Union, Optional, Iterable

from ampel.db.AmpelDB import AmpelDB
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsStockSelector(metaclass=AmpelABC):
	""" """

	@abstractmethod
	def __init__(self,
		# Note: change first parameter to "ampel_config: AmpelConfig" 
		# if stock selectors should require access to resources in the future
		ampel_db: AmpelDB,
		init_config: Optional[Union[BaseModel, Dict[str, Any]]] = None,
		options: Optional[Union[BaseModel, Dict[str, Any]]] = None
	):
		""" """

	@abstractmethod
	def get(self, 
		run_config: Union[BaseModel, Dict[str, Any]], 
		logger: AmpelLogger
	) -> Iterable:
		""" """
