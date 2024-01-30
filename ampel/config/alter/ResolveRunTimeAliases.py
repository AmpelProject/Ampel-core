#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/alter/ResolveRunTimeAliases.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.04.2023
# Last Modified Date:  05.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.abstract.AbsConfigUpdater import AbsConfigUpdater
from ampel.core.AmpelContext import AmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.util.recursion import walk_and_process_dict


class ResolveRunTimeAliases(AbsConfigUpdater):

	def alter(self, context: AmpelContext, content: dict[str, Any], logger: AmpelLogger) -> dict[str, Any]:

		# Run-time aliases generated at T4 for T0/T1 processes
		if context.run_time_aliases:
			walk_and_process_dict(
				arg = content,
				callback = self._gather_run_time_aliases_callback,
				run_time_aliases = context.run_time_aliases,
				logger = logger
			)

		return content

	def _gather_run_time_aliases_callback(self, path, current_key, current_d, **kwargs) -> None:
		""" Used by walk_and_process_dict(...) from morph(...) """
		# print(f"# path: {path}\n# d: {d}\n")
		for k, v in current_d.items():
			if isinstance(v, str) and v[0] == '%' == v[1]:
				for rt_key in kwargs['run_time_aliases']:
					if v == rt_key:
						if kwargs['logger'].verbose:
							kwargs['logger'].info(
								f"Setting value for run time alias {rt_key} with path {path}.{k}"
							)
						current_d[k] = kwargs['run_time_aliases'][rt_key]
