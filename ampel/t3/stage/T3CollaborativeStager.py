#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3CollaborativeStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.04.2021
# Last Modified Date: 23.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Union, List, Dict, Optional, Generator, Sequence, Set, Type, Iterable, Any
from ampel.view.SnapView import SnapView
from ampel.model.UnitModel import UnitModel
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3Unit import AbsT3Unit, T
from ampel.util.freeze import recursive_freeze
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T3Send
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater


class SimpleGenerator(BaseViewGenerator[T]):

	def __init__(self, unit: AbsT3Unit, views: Iterable[T], stock_updr: MongoStockUpdater) -> None:
		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.views = views

	def __iter__(self) -> Generator[T, T3Send, None]:
		l = self.stocks
		for v in self.views:
			l.append(v.id)
			yield v


class T3CollaborativeStager(T3BaseStager):
	"""
	A stager that calls t3 units 'process' methods sequentially.

	1) The source AmpelBuffer generator is fully consumed
	and buffers are converted to views which are stored in memory (all of them).
	Each T3 units is provided with a new generator based on those views.

	2) Results from upstream t3 units are made available for downstream units
	through 'session_info' which is updated after each unit execution
	"""

	session_info: Dict[str, Any] = {}

	#: t3 units (AbsT3Unit) to execute
	execute: Sequence[UnitModel]

	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		if self.logger.verbose > 1:
			self.logger.debug("Setting up T3CollaborativeStager")

		self.units = [
			self.get_unit(exec_def)
			for exec_def in self.execute
		]


	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Optional[Union[T3Document, List[T3Document]]]:
		""" Process AmpelBuffer instances """

		res: List[T3Document] = []
		for t3_unit, views in self.get_views(data).items():
			gen = SimpleGenerator(t3_unit, views, self.stock_updr)
			t3_unit.session_info.update(self.session_info) # type: ignore[union-attr]
			ts = time()
			if (ret := t3_unit.process(gen)):
				if (x := self.handle_t3_result(t3_unit, ret, gen.stocks, ts)):
					if (clsname := t3_unit.__class__.__name__) in self.session_info:
						if isinstance(self.session_info[clsname], dict):
							self.session_info[clsname] = [self.session_info[clsname], x['body']]
						else:
							self.session_info[clsname].append(x['body'])
					else:
						self.session_info[clsname] = x['body']
					res.append(x)
		return res


	def get_views(self, gen: Generator[AmpelBuffer, None, None]) -> Dict[AbsT3Unit, List[SnapView]]:

		Views: Set[Type[SnapView]] = {u._View for u in self.units}

		if len(Views) == 1:
			View = next(iter(Views))
			if self.paranoia:
				buffers = list(gen)
				return {
					unit: [View(**recursive_freeze(ab)) for ab in buffers]
					for unit in self.units
				}
			else:
				vs: List[SnapView] = [View(**recursive_freeze(ab)) for ab in buffers]
				return {unit: vs for unit in self.units}
		else:

			buffers = list(gen)
			if self.paranoia:
				return {
					unit: [View(**recursive_freeze(ab)) for ab in buffers]
					for unit, View in (lambda x: [(u, u._View) for u in x])(self.units)
				}

			else:

				optd: Dict[Type[SnapView], List[AbsT3Unit]] = {}
				for unit in self.units:
					if unit._View not in optd:
						optd[unit._View] = []
					optd[unit._View].append(unit)

				d = {}
				for View, units in optd.items():
					vs = [View(**recursive_freeze(ab)) for ab in buffers]
					for unit in units:
						d[unit] = vs
				return d
