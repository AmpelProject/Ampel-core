#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 19.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import importlib
from typing import Sequence, Dict, Any, Optional, Union, List, Literal, Set, Tuple
from ampel.type import ChannelId
from ampel.base.DataUnit import DataUnit
from ampel.abstract.ingest.AbsIngester import AbsIngester
from ampel.abstract.ingest.AbsStateT2Compiler import AbsStateT2Compiler
from ampel.abstract.ingest.AbsPointT2Compiler import AbsPointT2Compiler
from ampel.abstract.ingest.AbsStockT2Compiler import AbsStockT2Compiler
from ampel.model.ingest.T2IngestModel import T2IngestModel


class AbsT2Ingester(AbsIngester, abstract=True):

	compiler: Union[AbsStateT2Compiler, AbsPointT2Compiler, AbsStockT2Compiler]
	tags: Optional[List[Union[int, str]]]
	tier: Literal[0, 1, 3] = 0
	default_ingest_config: Dict = {}


	def add_ingest_models(self, channel: ChannelId, models: Sequence[T2IngestModel]):
		""" This method is typically called multiple times by the AP """

		for im in models:

			T2Class = None
			if isinstance(im.unit, str):
				unit_name = im.unit
				t2_info = self.context.config.get(f'unit.base.{unit_name}', dict)

				if not t2_info:
					raise ValueError(f'Unknown T2 unit {unit_name}')

				# The unit is installed locally
				if 'fqn' in t2_info:
					T2Class = self.context.loader.get_class_by_name(
						name = unit_name, unit_type = DataUnit
					)

				for el in t2_info['base']:
					# Pickup first Abstract class in mro()
					# Note: this strategy might cause pblms
					if el.startswith("Abs"):
						try:
							T2AbsClass = getattr(
								# import using fully qualified name
								importlib.import_module(f"ampel.abstract.{el}"), el
							)
						except Exception:
							raise ValueError(f"Unknown abstract class: {el}")

				if "T2AbsClass" not in locals(): # this should be unlikely to happen
					raise ValueError(f"No abstract class found for {unit_name}")
			else:
				T2Class = im.unit
				for base in T2Class.__mro__:
					if base.__name__.startswith("Abs"):
						T2AbsClass = base
						break
				else:
					raise ValueError(f"No abstract class found for {T2Class.__name__}")

			# Ingest options prevalence/priority from low to high:
			# 1) Default options from ingester (ex: T2PhotoIngester might default upper_limits to False)
			# 2) Default options from last T2 Abstract class (ex: AbsLightCurveT2Unit might default upper_limits to True)
			# 3) Default options from T2 class (ex: T2SNCosmo might default upper_limits to False)
			# 4) Specific options from model config (t2_compute)
			# (ex: custom T2SNCosmo ticket might request upper_limits nonetheless)
			ingest_options: Dict[str, Any] = {
				**self.default_ingest_config,
				**getattr(T2AbsClass, 'ingest', {}),
				**getattr(T2Class, 'ingest', {}),
				**(im.ingest if im.ingest else {})
			}

			self.compiler.add_ingest_model(channel, im)
			self.compiler.set_ingest_options(channel, im, ingest_options)


	@staticmethod
	def build_query_parts(chan_set: Set[ChannelId]) -> Tuple[
		Union[ChannelId, List[ChannelId]],
		Union[ChannelId, Dict[Literal['$each'], List[ChannelId]]]
	]:
		"""
		First tuple member is for the journal 'channel' field
		Second tuple member if for the operation $addToSet on root 'channel' field
		"""
		if len(chan_set) == 1:
			chan = next(iter(chan_set))
			return chan, chan
		else:
			chans = list(chan_set) # pymongo requires list
			return chans, {'$each': chans}
