#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsStockSelector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 09.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Dict, Any, Union, Sequence, Tuple

from ampel.db.AmpelDB import AmpelDB
from ampel.t3.TransientData import TransientData
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.t3.LoaderContentModel import LoaderContentModel


class AbsT3DataLoader(metaclass=AmpelABC):
	""" """

	class InitConfig(AmpelBaseModel):
		""" """
		content: Sequence[LoaderContentModel, str]
		channels: Union[None, AnyOf, AllOf, OneOf]


	@abstractmethod
	def __init__(self,
		# Note: change first parameter to "ampel_config: AmpelConfig" 
		# if data loaders should require access to resources in the future
		ampel_db: AmpelDB,
		init_config: Union[None, BaseModel, Dict[str, Any]] = None,
		**kwargs
	):
		""" """

	@abstractmethod
	def load(self, 
		stock_ids: Sequence[Union[int, str]],
		logger: AmpelLogger
	) -> Tuple[TransientData]:
		pass
