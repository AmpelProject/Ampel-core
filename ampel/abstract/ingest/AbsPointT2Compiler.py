#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsPointT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2020
# Last Modified Date: 30.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Union, Set, Tuple, List, Sequence, Optional
from ampel.base import abstractmethod
from ampel.type import DataPointId, ChannelId
from ampel.ingest.compile.CompilerBase import CompilerBase
from ampel.content.DataPoint import DataPoint

class AbsPointT2Compiler(CompilerBase, abstract=True):

	@abstractmethod
	def compile(self,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
		datapoints: Sequence[DataPoint]
	) -> Dict[Tuple[str, Optional[int], DataPointId], Set[ChannelId]]:
		pass
