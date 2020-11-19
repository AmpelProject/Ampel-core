#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStateT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2020
# Last Modified Date: 08.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Union, Set, Tuple, List, TypeVar, Generic, Optional
from ampel.base import abstractmethod
from ampel.type import ChannelId
from ampel.ingest.compile.CompilerBase import CompilerBase
from ampel.ingest.CompoundBluePrint import CompoundBluePrint

T = TypeVar("T", bound=CompoundBluePrint)

class AbsStateT2Compiler(Generic[T], CompilerBase, abstract=True):

	@abstractmethod
	def compile(self,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
		compound_blueprint: T
	) -> Dict[Tuple[str, Optional[int], Union[bytes, Tuple[bytes, ...]]], Set[ChannelId]]:
		"""
		Build a set of T2 documents to be created, de-duplicating those that
		are requested by multiple channels via :func:`add_ingest_model`
	
		:param chan_selection: channels to create T2 documents for
		:param compound_blueprint: compound creation plan
		:returns:
		  A dict whose keys are
		    - T2 unit name
		    - id of T2 unit configuration
		    - compound id
		  and whose values are a set of channel ids
		"""
		...
