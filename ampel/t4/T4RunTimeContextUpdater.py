#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t4/T4RunTimeContextUpdater.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                04.04.2023
# Last Modified Date:  04.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator, Sequence
from time import time
from typing import Annotated, Any

from ampel.abstract.AbsT4ControlUnit import AbsT4ControlUnit
from ampel.abstract.AbsT4Unit import AbsT4Unit
from ampel.content.T4Document import T4Document
from ampel.core.DocBuilder import DocBuilder
from ampel.model.UnitModel import UnitModel


class T4RunTimeContextUpdater(AbsT4ControlUnit, DocBuilder):

	execute: Annotated[Sequence[UnitModel], AbsT4Unit]
	allow_alias_override: bool = False

	def __init__(self, **kwargs) -> None:
		if isinstance(kwargs.get('execute'), dict):
			kwargs['execute'] = [kwargs['execute']]
		super().__init__(**kwargs)
			

	def do(self) -> Generator[T4Document, None, None]:

		aliases: dict[str, Any] = {}
		ts = time()
		for um in self.execute:
			t4_unit = self.context.loader.new_safe_logical_unit(
				um=um, unit_type=AbsT4Unit, logger=self.logger,
				_chan=self.channel # type: ignore[arg-type]
			)
			if ret := t4_unit.do():
				if not isinstance(ret, dict):
					raise ValueError(f'Invalid {um.unit} return value, dict expected')
				for k in ret:
					if not k[0] == '%' == k[1]:
						raise ValueError(
							f'Invalid run time alias returned by {um.unit}, '
							f'run time aliases must begin with %%'
						)
					if not self.allow_alias_override and k in aliases:
						raise ValueError(
							f'Run time alias {k} was already registered, '
							f'set allow_alias_override=True to ignore'
						)
				aliases |= ret

		for k, v in aliases.items():
			self.context.add_run_time_alias(k, v)

		yield self.craft_doc(self.event_hdlr, self, aliases, ts, doc_type=T4Document)
