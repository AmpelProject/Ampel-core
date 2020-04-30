#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 28.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union, Set, TypeVar, Generic, Optional
from ampel.types import ChannelId, T
from ampel.abc import abstractmethod
from ampel.abc.AmpelABC import AmpelABC
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.model.T2IngestModel import T2IngestModel

U = TypeVar("U")

class AbsT2Compiler(Generic[T, U], AmpelABC, abstract=True):
	"""
	T2 compilers optimize the ingestion of T2 documents into the DB.
	In particular, T2 documents shared among different channels are merged with each other.
	They must be configured first using the method `add_ingest_config`.
	Then, the method `compile` can be used to optimize the creation of T2 documents.
	"""

	@abstractmethod
	def add_ingest_config(self,
		channel: ChannelId,
		im: T2IngestModel,
		ingest_config: Dict[str, Any],
		logger: AmpelLogger
	) -> None:
		...

	@abstractmethod
	def compile(self,
		arg: T,
		chan_selection: Dict[ChannelId, Optional[Union[bool, int]]]
	) -> Dict[U, Set[ChannelId]]:
		...
