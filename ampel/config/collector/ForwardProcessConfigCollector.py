#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ForwardProcessConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 04.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Dict, Optional, Any, Sequence
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.AbsForwardConfigCollector import AbsForwardConfigCollector
from ampel.log import VERBOSE


class ForwardProcessConfigCollector(AbsForwardConfigCollector):

	def get_path(self, # type: ignore
		arg: Dict[str, Any],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str,
	) -> Optional[Sequence[Union[int, str]]]:

		if not isinstance(arg, dict) or 'tier' not in arg:
			self.error(
				"Unsupported process definition" +
				ConfigCollector.distrib_hint(dist_name, register_file)
			)
			return None

		path = ["process", "ops" if arg["tier"] is None else f"t{arg['tier']}"]

		if self.verbose:
			self.logger.log(VERBOSE,
				f"Routing process '{arg['name']}' to '{'.'.join(path)}'"
			)

		return path
