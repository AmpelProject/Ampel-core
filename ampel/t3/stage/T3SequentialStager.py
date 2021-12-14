#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3SequentialStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.04.2021
# Last Modified Date: 14.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Union, Generator, Sequence, Type, Iterable, Optional
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
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import dictify


class SimpleGenerator(BaseViewGenerator[T]):

	def __init__(self, unit: AbsT3ReviewUnit, views: Iterable[T], stock_updr: MongoStockUpdater) -> None:
		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.views = views

	def __iter__(self) -> Generator[T, T3Send, None]:
		l = self.stocks
		for v in self.views:
			l.append(v.id)
			yield v


class SkippableUnitModel(UnitModel):
	"""
	Whether unit result from DB should be used if avail.
	If compatible cached result is found,
	computation of the underlying T3 unit will be skipped
	"""
	cache: bool = False


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
	execute: Sequence[Union[UnitModel, SkippableUnitModel]]


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.units: list[AbsT3ReviewUnit] = []
		self.restorable_units: set[AbsT3ReviewUnit] = set()

		if self.logger.verbose > 1:
			self.logger.debug("Setting up T3CollaborativeStager")

		for model in self.execute:

			if isinstance(model, SkippableUnitModel):
				t3_unit = self.get_unit(
					UnitModel(unit=model.unit, config=model.config),
					self.channel
				)
				if model.cache:
					self.restorable_units.add(t3_unit)
			else:
				t3_unit = self.get_unit(model, self.channel)

			self.units.append(t3_unit)

		if self.restorable_units and self.propagate is False:
			raise ValueError("Restorable unit can only used in combination with propagate=True")


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> Optional[Generator[T3Document, None, None]]:

		for t3_unit, views in self.get_views(gen).items():

			if t3_unit in self.restorable_units:

				# Look up in views loaded by t3 supplier
				t3v = t3s.get_view(
					unit = t3_unit.__class__.__name__,
					config = t3_unit._trace_content
				)

				if t3v and (not t3v.stock or len(set(sv.id for sv in views) - set(t3v.stock)) == 0):
					self.logger.info(f"Omitting {t3_unit.__class__.__name__} run: results available in t3 store")
					continue

				# Try to fetch doc from DB (note that requiring t3 supplier to look for t3 docs while using
				# "cache: True" at the same time in SkippableUnitModel will result in the db being queried twice
				# -> try to use one or the other (alternative: remove cache feature and define a dedicated process template)
				t3v = self.get_cached_t3_view(t3_unit)
				if t3v and (not t3v.stock or len(set(sv.id for sv in views) - set(t3v.stock)) == 0):
					t3s.add_view(t3v)
					self.logger.info(f"Omitting {t3_unit.__class__.__name__} run: results fetched from DB")
					continue

			sg = SimpleGenerator(t3_unit, views, self.stock_updr)
			ts = time()

			if (ret := t3_unit.process(sg, t3s)):
				if (x := self.handle_t3_result(t3_unit, ret, t3s, sg.stocks, ts)):
					if self.propagate:
						t3s.add_view(
							T3DocView.of(x, self.context.config)
						)
					yield x


	def get_views(self, gen: Generator[AmpelBuffer, None, None]) -> dict[AbsT3ReviewUnit, list[SnapView]]:

		Views: set[Type[SnapView]] = {u._View for u in self.units}
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

				optd: dict[Type[SnapView], list[AbsT3ReviewUnit]] = {}
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


	def get_cached_t3_view(self, t3_unit: AbsT3ReviewUnit) -> Optional[T3DocView]:

		col = self.context.db.get_collection('t3')
		h = build_unsafe_dict_id(dictify(t3_unit._trace_content), ret=int)

		if self.resolve_config:
			for el in col.find({'unit': t3_unit.__class__.__name__}):
				if el['confid'] == h:
					return T3DocView.of(el, self.context.config)
		else:
			if (d := next(iter(col.find({'unit': t3_unit.__class__.__name__, 'confid': h})), None)):
				return T3DocView.of(d, self.context.config)

		return None
