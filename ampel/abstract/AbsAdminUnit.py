#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsProcessorUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 18.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from pydantic import BaseModel, root_validator, Extra
from ampel.abc import defaultmethod
from ampel.abc.AmpelABC import AmpelABC
from ampel.core.AmpelContext import AmpelContext


class AbsProcessorUnit(AmpelABC, BaseModel, abstract=True):
	"""
	Top level abstract class containing a handle to an AmpelContext instance
	"""

	class Config:
		extra = Extra.forbid
		arbitrary_types_allowed = True

	context: AmpelContext
	process_name: Optional[str]
	verbose: bool = False
	debug: bool = False


	@defaultmethod(check_super_call=True)
	def __init__(self, **kwargs):
		self.__config__.extra = Extra.forbid
		super().__init__(**kwargs)
		self.__config__.extra = Extra.allow


	@root_validator
	def _set_defaults(cls, values):
		if values.get('debug'):
			values['verbose'] = True
		return values
