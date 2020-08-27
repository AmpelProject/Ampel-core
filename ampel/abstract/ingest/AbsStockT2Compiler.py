#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsStockT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2020
# Last Modified Date: 08.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Union, Set, Tuple, List, Optional
from ampel.base import abstractmethod
from ampel.type import ChannelId
from ampel.ingest.compile.CompilerBase import CompilerBase


class AbsStockT2Compiler(CompilerBase, abstract=True):

	@abstractmethod
	def compile(self,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
	) -> Dict[Tuple[str, Optional[int]], Set[ChannelId]]:
		...
