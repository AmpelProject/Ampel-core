#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/ProcessTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.10.2019
# Last Modified Date: 16.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.utils.mappings import flatten_dict, unflatten_dict

class ProcessTemplate(AmpelStrictModel):

	tier: int
	template: str
	customize: Dict[str, Any]

	def dict(self, # type: ignore[override]
		by_alias=None, skip_defaults=None, include=None, exclude=None
	) -> Dict[str, Any]:

		# Avoid cyclic import troubles
		from ampel.config.builder.ConfigBuilder import ConfigBuilder

		if self.template not in ConfigBuilder.templates:
			raise ValueError(f"Template {self.template} not found")

		flat_custo = flatten_dict(self.customize)

		tpl_content = ConfigBuilder.templates[self.template].copy()
		tpl_config = tpl_content.pop("tplConfig")
		flat_tpl = flatten_dict(tpl_content)

		if "aliases" in tpl_config:

			# not changing dict keys while iterating over it
			translate_keys = {
				ck: ck.replace(k, v)
				for k, v in tpl_config['aliases'].items()
					for ck in flat_custo.keys()
						if ck.startswith(k)
			}

			for old, new in translate_keys.items():
				flat_custo[new] = flat_custo.pop(old)

		if "customizable" in tpl_config:
			for custo_key in flat_custo:
				if not any([
					custo_key.startswith(allowed_key)
					for allowed_key in tpl_config['customizable']
				]):
					raise ValueError(f"Key {custo_key} cannot be customized")

		return unflatten_dict(
			{**flat_tpl, **flat_custo}
		)
