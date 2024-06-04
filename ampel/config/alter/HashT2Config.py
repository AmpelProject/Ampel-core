#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/alter/HashT2Config.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.04.2023
# Last Modified Date:  05.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.abstract.AbsConfigUpdater import AbsConfigUpdater
from ampel.config.builder.ProcessMorpher import ProcessMorpher
from ampel.config.collector.T02ConfigCollector import T02ConfigCollector
from ampel.core.AmpelContext import AmpelContext
from ampel.log.AmpelLogger import AmpelLogger


class HashT2Config(AbsConfigUpdater):
	""" Hashes t2 configs recursively """

	def alter(self, context: AmpelContext, content: dict[str, Any], logger: AmpelLogger) -> dict[str, Any]:

		pm = ProcessMorpher(process={}, logger=logger, proc_name='<off-config>')
		cc = T02ConfigCollector(conf_section="confid")
		# Note: *process* templating could be possible there as well, add if ever needed
		ac = context.config._config  # noqa: SLF001
		pm.hash_t2_config(
			{'unit': ac['unit'], 'alias': ac['alias'], 'confid': cc},
			target=content['directives'] if 'directives' in content else content['config']['directives']
		)

		s = []
		for conf_id, conf in cc.items():
			if conf_id not in context.db.conf_ids:
				s.append(conf_id)
				context.db.add_conf_id(conf_id, conf)
				dict.__setitem__(ac["confid"], conf_id, conf)

		if logger.verbose:
			logger.info("DB updated with new conf ids", extra={'confids': s})

		return content
