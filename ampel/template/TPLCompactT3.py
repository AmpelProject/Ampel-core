#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/template/TPLCompactT3.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.07.2021
# Last Modified Date:  11.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.types import OneOrMany
from ampel.util.pretty import prettyjson
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.UnitModel import UnitModel
from ampel.model.t3.T3IncludeDirective import T3IncludeDirective
from ampel.model.t3.T3DocBuilderModel import T3DocBuilderModel
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate


class TPLCompactT3(AbsProcessorTemplate, T3DocBuilderModel):
	"""
	Enables compact run blocks such as:

	resolve_config: True
	human_id: [unit, tag]
	execute:
	- supply: ...
	  stage: ...

	- unit: T3DistanceModulus
	  config:
	    redshifts: [0.015, 0.035, 0.045, 0.06, 0.1]

	- unit: T3HubbleResidual
	  resolve_config: False
	  config:
	    plot_tag: ZTF


	Output:

	execute:
	- unit: T3ReviewUnitExecutor
	  config:
	    supply: ...
	    stage: ...

	- unit: T3PlainUnitExecutor
	  config:
	    resolve_config: True
	    human_id: [unit, tag]
	    target:
	      unit: T3DistanceModulus
	      config:
	        redshifts: [0.015, 0.035, 0.045, 0.06, 0.1]

	- unit: T3PlainUnitExecutor
	  config:
	    resolve_config: False
	    human_id: [unit, tag]
	    target:
	      unit: T3HubbleResidual
	      config:
	        plot_tag: ZTF


	Note that T3DocBuilderModel parameters set at root level will be applied for each run block
	(unless dedicated overrides are defined within specific run blocks)
	"""

	include: None | T3IncludeDirective
	execute: OneOrMany[dict[str, Any]]

	def _merge_confs(self, d: dict[str, Any]) -> dict[str, Any]:
		bmks = T3DocBuilderModel.get_model_keys()
		return {k: v for k, v in self.dict(exclude_defaults=True).items() if k in bmks} | \
			{k: v for k, v in d.items() if k in bmks}


	# Mandatory override
	def get_model(self, config: dict[str, Any], logger: AmpelLogger) -> UnitModel[str]:

		out: list[dict] = []
		units = config['unit']

		for el in [self.execute] if isinstance(self.execute, dict) else self.execute:

			if 'supply' in el and 'stage' in el:
				out.append(
					{
						'unit': "T3ReviewUnitExecutor",
						'config': self._merge_confs(el) | {'supply': el['supply'], 'stage': el['stage']}
					}
				)
				continue

			if 'unit' in el:
				
				if el['unit'] not in units:
					raise ValueError(f"Unknown unit: {el['unit']}")

				if "AbsT3PlainUnit" in units[el['unit']]['base']:
					out.append(
						{
							'unit': 'T3PlainUnitExecutor',
							'config': self._merge_confs(el) | {
								# Enable non-standard UnitModel fields (ex: T3SkippableUnitModel.cache)
								# 'target': {'unit': el['unit'], 'config': el['config']}
								'target': {k: v for k, v in el.items() if k not in T3DocBuilderModel.get_model_keys()}
							}
						}
					)
					continue

				elif "AbsT3ControlUnit" in units[el['unit']]['base']:
					out.append(el)
					continue

			raise ValueError("Run block syntax unsupported: \n" + prettyjson(el))

		conf: dict[str, Any] = {'include': self.include} if self.include else {}
		conf['execute'] = out

		return UnitModel(unit = 'T3Processor', config = conf)
