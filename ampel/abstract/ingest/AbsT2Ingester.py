#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/ingest/AbsT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 24.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib
from typing import Sequence, Dict, Any, Optional, Union, List, Literal
from ampel.types import ChannelId
from ampel.abstract.ingest.AbsIngester import AbsIngester
from ampel.abstract.ingest.AbsT2Compiler import AbsT2Compiler
from ampel.model.T2IngestModel import T2IngestModel


class AbsT2Ingester(AbsIngester, abstract=True):

	run_id: int
	compiler: AbsT2Compiler
	tags: Optional[List[Union[int, str]]]
	tier: Literal[0, 1, 3] = 0
	default_options: Dict = {}


	def add_ingest_models(self, channel: ChannelId, models: Sequence[T2IngestModel]):
		"""
		This method is usually called multiple times by the AP
		"""

		if self.debug:
			self.logger.info(
				'Loading config',
				extra={'channel': channel}
			)

		for im in models:

			T2Class = None
			unit_name = im.unit
			t2_info = self.context.config.get(f't2.unit.base.{unit_name}', dict)

			if not t2_info:
				raise ValueError(f'Unknown T2 unit {unit_name}')

			# The unit is installed locally
			if 'fqn' in t2_info:
				T2Class = self.context.loader.get_class(
					class_name = unit_name,
					unit_type = 'base'
				)

			for el in t2_info['abc']:
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

			# Ingest options prevalence/priority from low to high:
			# 1) Default options from ingester (ex: T2PhotoIngester might default upper_limits to False)
			# 2) Default options from last T2 Abstract class (ex: AbsLightCurveT2Unit might default upper_limits to True)
			# 3) Default options from T2 class (ex: T2SNCosmo might default upper_limits to False)
			# 4) Specific options from model config (t2_compute)
			# (ex: custom T2SNCosmo ticket might request upper_limits nonetheless)
			ingest_config: Dict[str, Any] = {
				**self.default_options,
				**getattr(T2AbsClass, 'ingest', {}),
				**getattr(T2Class, 'ingest', {}),
				**(im.ingest if im.ingest else {})
			}

			self.compiler.add_ingest_config(channel, im, ingest_config, self.logger)
