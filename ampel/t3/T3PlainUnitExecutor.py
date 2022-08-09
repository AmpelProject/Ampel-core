#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/T3PlainUnitExecutor.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                12.12.2021
# Last Modified Date:  13.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from typing import Annotated
from collections.abc import Generator

from ampel.types import ChannelId
from ampel.view.T3Store import T3Store
from ampel.view.T3DocView import T3DocView
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit
from ampel.base.AmpelUnit import AmpelUnit
from ampel.t3.T3DocBuilder import T3DocBuilder
from ampel.content.T3Document import T3Document
from ampel.model.UnitModel import UnitModel
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import dictify
from ampel.util.serialize import walk_and_encode


class SkippableUnitModel(UnitModel):
	"""
	Whether unit result from DB should be used if avail.
	If compatible cached result is found,
	computation of the underlying T3 unit will be skipped

	Potentialy save hash of dependent unit result (body) in meta record (extra.unit#confid).
	"""
	cache: None | UnitModel = None


class T3PlainUnitExecutor(AbsT3ControlUnit, T3DocBuilder):

	# Require single channel for now (super classes allow multi-channel)
	channel: None | ChannelId = None

	target: Annotated[SkippableUnitModel, AbsT3PlainUnit]

	def process(self, t3s: T3Store) -> None | Generator[T3Document, None, None]:

		t3_unit = self.context.loader.new_safe_logical_unit(
			UnitModel(unit=self.target.unit, config=self.target.config),
			unit_type = AbsT3PlainUnit,
			logger = self.logger,
			_chan = self.channel
		)

		if self.target.cache:

			col = self.context.db.get_collection('t3')
			h = self.get_target_cache_config()

			if view := t3s.get_view(unit=self.target.cache.unit, config=h):
				if (body := view.get_body()) and isinstance(body, dict):
					matchd = {
						'unit': self.target.unit,
						'confid': build_unsafe_dict_id(t3_unit._get_trace_content()),
						f'meta.extra.{self.target.cache.unit}#{h}': build_unsafe_dict_id(
							walk_and_encode(dictify(body), destructive=False)
						)
					}
					if (d := next(iter(col.find(matchd)), None)):
						self.logger.info("Using cached result", extra=matchd)
						t3s.add_view(
							T3DocView.of(d, self.context.config)
						)
						return None

		self.logger.info("Running T3 unit", extra={'unit': self.target.unit})
		ts = time()
		ret = t3_unit.process(t3s)
		self.flush(t3_unit)

		if ret and (x := self.handle_t3_result(t3_unit, ret, t3s, None, ts)):

			if self.target.cache and isinstance(x['body'], dict):

				if 'extra' not in x['meta']:
					x['meta']['extra'] = {}

				h = self.get_target_cache_config()
				if view := t3s.get_view(unit=self.target.cache.unit, config=h):
					x['meta']['extra'][f"{self.target.cache.unit}#{h}"] = build_unsafe_dict_id(
						walk_and_encode(dictify(view.get_body()), destructive=False)
					)

			yield x


	def get_target_cache_config(self) -> int:

		if self.target.cache:
			if isinstance(self.target.cache.config, dict):
				vc = self.context \
					.loader.get_class_by_name(self.target.cache.unit, unit_type=AmpelUnit) \
					.validate(self.target.cache.config)

				return build_unsafe_dict_id(dictify(vc))
			elif isinstance(self.target.cache.config, int):
				return self.target.cache.config
			else:
				print(type(self.target.cache.config))

		raise ValueError("Bad config")
