#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStateT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2020
# Last Modified Date: 08.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Union, Set, Tuple, List, TypeVar, Generic
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
	) -> Dict[Tuple[str, int, Union[bytes, Tuple[bytes, ...]]], Set[ChannelId]]:
		...
