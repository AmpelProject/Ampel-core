#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TranLoadConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Dict, Union, List
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.T3LoadableDocs import T3LoadableDocs

@gendocstring
class T3TranLoadConfig(BaseModel):
	""" """
	state: str = "$latest"
	docs: Union[T3LoadableDocs, List[T3LoadableDocs]]
	t2s: Union[str, List[str]]
	verbose: bool = True
	debug: bool = False
