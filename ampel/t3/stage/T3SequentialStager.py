#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3CollaborativeStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.04.2021
# Last Modified Date: 26.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Union, List, Dict, Generator, Sequence, Set, Type, Iterable, Any
from ampel.types import UBson
from ampel.view.SnapView import SnapView
from ampel.model.UnitModel import UnitModel
from ampel.t3.T3CachedResult import T3CachedResult
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3Unit import AbsT3Unit, T
from ampel.util.freeze import recursive_freeze
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T3Send
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import dictify


class SimpleGenerator(BaseViewGenerator[T]):

	def __init__(self, unit: AbsT3Unit, views: Iterable[T], stock_updr: MongoStockUpdater) -> None:
		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.views = views

	def __iter__(self) -> Generator[T, T3Send, None]:
		l = self.stocks
		for v in self.views:
			l.append(v.id)
			yield v


class ExtendedUnitModel(UnitModel):

	#: Whether unit result from DB should be used if avail.
	#: If compatible cached result is found,
	#: computation of the underlying T3 unit will be skipped
	cache: bool = False


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
	execute: Sequence[Union[UnitModel, ExtendedUnitModel]]


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.units: list[AbsT3Unit] = []

		if self.logger.verbose > 1:
			self.logger.debug("Setting up T3CollaborativeStager")

		for model in self.execute:

			if isinstance(model, ExtendedUnitModel):

				t3_unit = self.get_unit(
					UnitModel(unit = model.unit, config = model.config),
					self.channel
				)

				if model.cache:

					d = None
					col = self.context.db.get_collection('t3')
					h = build_unsafe_dict_id(
						dictify(t3_unit._trace_content), ret = int
					)

					if self.resolve_config:
						for el in col.find({'unit': model.unit}):
							if build_unsafe_dict_id(el['config'], ret = int) == h:
								d = el
								break
					else:
						d = next(iter(col.find({'unit': model.unit, 'config': h})), None)

					if d:
						self.units.append(
							T3CachedResult(
								unit = model.unit,
								confid = h,
								content = d.get('body'),
								logger = t3_unit.logger,
								session_info = self.session_info
							)
						)
						continue
			else:
				t3_unit = self.get_unit(model, self.channel)

			self.units.append(t3_unit)


	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Generator[T3Document, None, None]:
		""" Process AmpelBuffer instances """

		for t3_unit, views in self.get_views(data).items():

			if isinstance(t3_unit, T3CachedResult):
				self.update_session(t3_unit.unit, t3_unit.process(None))
				continue

			gen = SimpleGenerator(t3_unit, views, self.stock_updr)
			t3_unit.session_info.update(self.session_info) # type: ignore[union-attr]
			ts = time()

			if (ret := t3_unit.process(gen)):
				if (x := self.handle_t3_result(t3_unit, ret, gen.stocks, ts)):
					self.update_session(t3_unit.__class__.__name__, x['body'])
					yield x


	def update_session(self, clsname: str, body: UBson) -> None:
		if clsname in self.session_info:
			if isinstance(self.session_info[clsname], dict):
				self.session_info[clsname] = [self.session_info[clsname], body]
			else:
				self.session_info[clsname].append(body)
		else:
			self.session_info[clsname] = body


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
