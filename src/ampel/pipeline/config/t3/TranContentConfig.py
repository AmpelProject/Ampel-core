#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/TranContentConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 02.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator, ValidationError
from typing import Dict, Union, List
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.GettableConfig import GettableConfig
from ampel.pipeline.config.t3.LoadableContent import LoadableContent

@gendocstring
class TranContentConfig(BaseModel, GettableConfig):
	""" """
	docs: Union[LoadableContent, List[LoadableContent]]
	t2SubSelection: Union[None, str, List[str]] = None


	@validator('docs', 't2SubSelection', pre=True, whole=True)
	def make_it_a_list(cls, v):
		if type(v) is not list:
			return [v]
		return v


	@validator('t2SubSelection', whole=True)
	def check_correct_use_of_subselection(cls, t2SubSelection, values, **kwargs):
		"""
		Check transients->select->t2s
		"""

		if t2SubSelection and values.get("docs"):

			docs = values.get("docs")
			if not AmpelUtils.is_sequence(docs):
				docs = [docs]

			if LoadableContent.T2RECORD not in docs:
				AmpelUtils.print_and_raise(
					"T3 config error",
					"T2RECORD must be defined in transients->select->docs\n"+
					"when transients->content->t2SubSelection (%s) filtering\nis requested." % t2SubSelection
				)

		return t2SubSelection
