#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT3Loader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.12.2019
# Last Modified Date: 21.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Iterable, Sequence, Optional, Dict, List, Iterator
from ampel.types import StockId, ChannelId, StrictIterable
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.core.ContextUnit import ContextUnit, AmpelContext
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.core.DataLoader import DataLoader
from ampel.model.t3.LoaderDirective import LoaderDirective
from ampel.log.AmpelLogger import AmpelLogger


# Data loaders need access to the ampel db (and hence inherits ContextUnit)
class AbsT3Loader(AmpelABC, ContextUnit, abstract=True):
	"""
	Base class for loading documents associated with a set of stocks.
	"""

	logger: AmpelLogger

	#: Specification of documents to load. If these are supplied as strings,
	#: they will be resolved by retrieving the corresponding alias from the
	#: Ampel config.
	directives: Sequence[LoaderDirective]

	#: Channels to load documents for
	channel: Optional[
		Union[
			ChannelId,
			AnyOf[ChannelId],
			AllOf[ChannelId],
			OneOf[ChannelId]
		]
	]


	def __init__(self, context: AmpelContext, **kwargs) -> None:

		# Note: 'directives' in kwargs can contain strings which will be
		# resolved by retrieving the associated alias from the ampel config
		directives: List[Dict] = []

		# Resolve directive aliases
		for el in kwargs.get('directives', []):
			if isinstance(el, str):
				d = context.config.get(f"alias.t3.%{el}", dict)
				if d: # mypy does not yet support type inference using the walrus operator
					directives.append(d)
				else:
					raise ValueError(f"LoaderDirective alias '{el}' not found in ampel config")
			else:
				directives.append(el)

		kwargs['directives'] = tuple(directives)

		# No need to save context as instance variable
		super().__init__(context=context, **kwargs)

		self.data_loader = DataLoader(context)


	@abstractmethod
	def load(self,
		stock_ids: Union[StockId, Iterator[StockId], StrictIterable[StockId]]
	) -> Iterable[AmpelBuffer]:
		""" Load documents (collection Ampel_data) for the selected stocks """
		...
