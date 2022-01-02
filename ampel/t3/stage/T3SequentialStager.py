#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3SequentialStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                22.04.2021
# Last Modified Date:  14.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from collections.abc import Generator, Iterable, Sequence
from ampel.view.T3Store import T3Store
from ampel.view.T3DocView import T3DocView
from ampel.view.SnapView import SnapView
from ampel.model.UnitModel import UnitModel
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit, T
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T3Send
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater


class SimpleGenerator(BaseViewGenerator[T]):

	def __init__(self, unit: AbsT3ReviewUnit, views: Iterable[T], stock_updr: MongoStockUpdater) -> None:
		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.views = views

	def __iter__(self) -> Generator[T, T3Send, None]:
		l = self.stocks
		for v in self.views:
			l.append(v.id)
			yield v


class T3SequentialStager(T3BaseStager):
	"""
	A stager that calls t3 units 'process' methods sequentially.

	1) The source AmpelBuffer generator is fully consumed
	and buffers are converted to views which are stored in memory (all of them).
	Each T3 units is provided with a new generator based on those views.

	2) Results from upstream t3 units are made available for downstream units
	through the t3 store instance which is updated after each unit execution
	unless propagate is set to False
	"""

	propagate: bool = True

	#: t3 units (AbsT3ReviewUnit) to execute
	execute: Sequence[UnitModel]


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		if self.logger.verbose > 1:
			self.logger.debug(f"Setting up {self.__class__.__name__}")

		self.units = [
			self.get_unit(model, self.channel)
			for model in self.execute
		]


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:

		for t3_unit, views in self.get_views(gen).items():

			sg = SimpleGenerator(t3_unit, views, self.stock_updr)
			if (ret := t3_unit.process(sg, t3s)):
				if (x := self.handle_t3_result(t3_unit, ret, t3s, sg.stocks, time())):
					if self.propagate:
						t3s.add_view(
							T3DocView.of(x, self.context.config)
						)
					yield x


	def get_views(self, gen: Generator[AmpelBuffer, None, None]) -> dict[AbsT3ReviewUnit, list[SnapView]]:

		Views: set[type[SnapView]] = {u._View for u in self.units}
		conf = self.context.config

		if len(Views) == 1:
			View = next(iter(Views))
			if self.paranoia:
				buffers = list(gen)
				return {
					unit: [View.of(ab, conf) for ab in buffers]
					for unit in self.units
				}
			else:
				vs: list[SnapView] = [View.of(ab, conf) for ab in buffers]
				return {unit: vs for unit in self.units}
		else:

			buffers = list(gen)
			if self.paranoia:
				return {
					unit: [View.of(ab, conf) for ab in buffers]
					for unit, View in (lambda x: [(u, u._View) for u in x])(self.units)
				}

			else:

				optd: dict[type[SnapView], list[AbsT3ReviewUnit]] = {}
				for unit in self.units:
					if unit._View not in optd:
						optd[unit._View] = []
					optd[unit._View].append(unit)

				d = {}
				for View, units in optd.items():
					vs = [View.of(ab, conf) for ab in buffers]
					for unit in units:
						d[unit] = vs
				return d
