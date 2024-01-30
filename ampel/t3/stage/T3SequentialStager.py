#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3SequentialStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                22.04.2021
# Last Modified Date:  03.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator, Iterable, Sequence
from time import time
from typing import Annotated

from ampel.abstract.AbsT3Unit import AbsT3Unit, T
from ampel.content.T3Document import T3Document
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.struct.T3Store import T3Store
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T3Send
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.view.SnapView import SnapView
from ampel.view.T3DocView import T3DocView


class SimpleGenerator(BaseViewGenerator[T]):

	def __init__(self, unit: AbsT3Unit, views: Iterable[T], stock_updr: MongoStockUpdater) -> None:
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

	#: t3 units to execute
	execute: Annotated[Sequence[UnitModel], AbsT3Unit]


	def __init__(self, **kwargs) -> None:

		if isinstance(kwargs.get('execute'), dict):
			kwargs['execute'] = [kwargs['execute']]

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
			ts = time()

			if (
				(ret := t3_unit.process(sg, t3s)) and
				(x := self.handle_t3_result(t3_unit, ret, t3s, sg.stocks, ts))
			):
				if self.propagate:
					t3s.add_view(
						T3DocView.of(x, self.context.config)
					)
				yield x

		return None


	def get_views(self, gen: Generator[AmpelBuffer, None, None]) -> dict[AbsT3Unit, list[SnapView]]:

		Views: set[type[SnapView]] = {u._View for u in self.units}  # noqa: SLF001
		conf = self.context.config

		if len(Views) == 1:
			View = next(iter(Views))
			if self.paranoia_level:
				buffers = list(gen)
				return {
					unit: [View.of(ab, conf) for ab in buffers]
					for unit in self.units
				}
			vs: list[SnapView] = [View.of(ab, conf) for ab in gen]
			return {unit: vs for unit in self.units}

		buffers = list(gen)
		if self.paranoia_level:
			return {
				unit: [View.of(ab, conf) for ab in buffers]
				for unit, View in ((u, u._View) for u in self.units)  # noqa: SLF001
			}

		optd: dict[type[SnapView], list[AbsT3Unit]] = {}
		for unit in self.units:
			if unit._View not in optd:  # noqa: SLF001
				optd[unit._View] = []  # noqa: SLF001
			optd[unit._View].append(unit)  # noqa: SLF001

		d = {}
		for View, units in optd.items():
			vs = [View.of(ab, conf) for ab in buffers]
			for unit in units:
				d[unit] = vs
		return d
